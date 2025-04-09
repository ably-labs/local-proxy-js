import type Ably from 'ably';
import http from 'http';
import httpProxy from 'http-proxy';
import WebSocket, { WebSocketServer } from 'ws';
import { AsyncQueue } from './async-queue.js';
import { CompletableDeferred } from './completable-deferred';

export interface ProtocolMessage {
  action?: number;
  flags?: number;
  id?: string;
  timestamp?: number;
  count?: number;
  error?: Ably.ErrorInfo;
  connectionId?: string;
  channel?: string;
  channelSerial?: string | null;
  msgSerial?: number;
  messages?: Ably.Message[];
  presence?: Ably.PresenceMessage[];
  auth?: unknown;
  connectionDetails?: Record<string, unknown>;
}

export interface Request<T = any> {
  headers: http.IncomingHttpHeaders;
  data: T;
  url: string | undefined;
  params: URLSearchParams;
}

export interface Response<T = any> {
  headers: http.IncomingHttpHeaders;
  statusCode?: number;
  data: T;
}

export interface RealtimeMiddlewareCommand {
  command: 'drop' | 'replace' | 'keep';
  replacement?: ProtocolMessage | undefined;
}

export type MessageType = 'incoming' | 'outgoing';

type RestMiddleware<REQ = any, RES = any> = (req: Request<REQ>, res: Response<RES>) => Promise<Response<RES>>;
type RealtimeMiddleware = (message: ProtocolMessage, messageType: MessageType) => Promise<RealtimeMiddlewareCommand>;

/**
 * Local proxy for test purposes. Core functionality includes:
 *
 * - Spying on HTTP Requests and Responses
 *   - Inspect outgoing HTTP requests from the client.
 *   - Inspect and modify incoming HTTP responses.
 *
 * - Spying on WebSocket Messages
 *   - Observe outgoing WebSocket messages from the client.
 *   - Inspect and modify incoming WebSocket messages.
 *
 * - Injecting WebSocket Messages
 *   - Simulate server messages by injecting WebSocket frames into the client session.
 *
 * - Breaking WebSocket and HTTP Connections
 *   - Force WebSocket disconnections to test reconnection behavior.
 *   - Simulate slow or broken HTTP responses to test timeout handling.
 */
export interface InterceptingProxy {
  /**
   * Options that can be used to configure an Ably Realtime or Rest client
   * to point to this proxy instead of directly to Ably backend.
   *
   * Includes overridden `restHost`, `realtimeHost`, and `tls` settings
   * so SDK traffic can be routed through the proxy.
   */
  get options(): Ably.ClientOptions;

  /**
   * Starts the proxy server. This must be called before intercepting any traffic.
   *
   * @returns A promise that resolves once the proxy is running and ready.
   */
  start(): Promise<void>;

  /**
   * Observes the next outgoing HTTP request made by the client.
   *
   * @param filter Optional predicate function to filter observed requests.
   * @returns A promise that resolves with the matching request object.
   */
  observeNextRequest<T = any>(filter?: (req: Request<T>) => boolean): Promise<Request<T>>;

  /**
   * Observes the next outgoing Ably protocol message from the client.
   *
   * @param filter Optional predicate function to filter observed messages.
   * @returns A promise that resolves with the matching protocol message.
   */
  observeNextOutgoingProtocolMessage(filter?: (msg: ProtocolMessage) => boolean): Promise<ProtocolMessage>;

  /**
   * Observes the next incoming HTTP response received by the client.
   *
   * @param filter Optional predicate function to filter observed responses.
   * @returns A promise that resolves with the matching response object.
   */
  observeNextResponse<T = any>(filter?: (res: Response<T>) => boolean): Promise<Response<T>>;

  /**
   * Observes the next Ably protocol message received from the server.
   *
   * @param filter Optional predicate function to filter observed messages.
   * @returns A promise that resolves with the matching protocol message.
   */
  observeNextIncomingProtocolMessage(filter?: (msg: ProtocolMessage) => boolean): Promise<ProtocolMessage>;

  /**
   * Replaces the next incoming Ably protocol message with a custom one.
   *
   * Can be used to simulate modified or corrupted server messages.
   *
   * @param replacement The message to inject in place of the real one.
   * @param filter Optional predicate function to filter which message to replace.
   */
  replaceNextIncomingProtocolMessage(replacement: ProtocolMessage, filter?: (msg: ProtocolMessage) => boolean): Promise<void>;

  /**
   * Injects a protocol message into the client's connection as if it was sent from the server.
   *
   * Useful for simulating specific server-side events or message sequences.
   *
   * @param connectionId The ID of the connection to inject the message into.
   * @param msg The protocol message to inject.
   */
  injectProtocolMessage(connectionId: string, msg: ProtocolMessage): void;

  /**
   * Drops the connection with the specified ID.
   *
   * Forces a WebSocket disconnection to simulate network interruptions or server disconnects.
   *
   * @param connectionId The ID of the connection to drop.
   */
  dropConnection(connectionId: string): void;

  /**
   * Pauses all network activity for the active connection.
   *
   * Can be used to simulate network latency or stalled connections.
   * Returns a function that, when called, resumes the connection.
   *
   * @returns A function to resume the paused connection.
   */
  pauseAllConnections(): () => void;

  /**
   * Registers a middleware function to inspect or modify HTTP REST requests and responses.
   *
   * @param middleware A function to intercept and optionally modify REST traffic.
   * @returns A function to unregister the middleware.
   */
  registerRestMiddleware(middleware: RestMiddleware): () => void;

  /**
   * Registers a middleware function to inspect or modify WebSocket (Realtime) messages.
   *
   * @param middleware A function to intercept and optionally modify protocol messages.
   * @returns A function to unregister the middleware.
   */
  registerRealtimeMiddleware(middleware: RealtimeMiddleware): () => void;
}

class DefaultInterceptingProxy implements InterceptingProxy {
  private readonly realtimeClientOptions: Ably.ClientOptions;
  private readonly restMiddlewares: RestMiddleware[] = [];
  private readonly realtimeMiddlewares: RealtimeMiddleware[] = [];
  private restQueue = new AsyncQueue();
  private realtimeQueue = new AsyncQueue();
  private connectionIdToWs = new Map<string, WebSocket>();

  private readonly proxy = httpProxy.createProxyServer({});
  private readonly wss = new WebSocketServer({ noServer: true });

  private readonly server: http.Server = http.createServer((req: http.IncomingMessage, res: http.ServerResponse) => {
    this.proxy.web(req, res, { target: this.targetRestUrl, selfHandleResponse: true }, (err) => console.error(err));

    const requestChunks: Buffer[] = [];

    this.proxy.once('proxyRes', (proxyRes, req, res) => {
      const responseChunks: Buffer[] = [];

      proxyRes.on('data', (chunk) => responseChunks.push(chunk));
      proxyRes.on('end', () => {
        try {
          const responseData = JSON.parse(Buffer.concat(responseChunks).toString());
          const requestData = JSON.parse(Buffer.concat(requestChunks).toString());

          this.restQueue.enqueue(async () => {
            const modifiedResponse = await this.applyRestMiddlewares(
              {
                headers: req.headers,
                data: requestData,
                url: req.url,
                params: new URLSearchParams(req.url),
              },
              {
                headers: proxyRes.headers,
                data: responseData,
                statusCode: proxyRes.statusCode || 200,
              },
            );
            res.writeHead(modifiedResponse.statusCode || 200, modifiedResponse.headers);
            res.end(JSON.stringify(modifiedResponse.data));
          });
        } catch (e: unknown) {
          res.writeHead(500, proxyRes.headers);
          res.end(`Proxy error: ${e}`);
        }
      });
    });

    req.on('data', (chunk) => requestChunks.push(chunk));
  });

  private serverIsRunning = false;
  private port = 21345;

  constructor(realtimeClientOptions: Ably.ClientOptions) {
    this.realtimeClientOptions = realtimeClientOptions;

    this.server.on('upgrade', (req, socket, head) => {
      this.wss.handleUpgrade(req, socket, head, (ws) => {
        const targetWs = new WebSocket(this.targetRealtimeUrl);
        const deferredValue = CompletableDeferred<void>();
        targetWs.on('open', () => deferredValue.complete());

        ws.on('message', (message) => {
          let data: ProtocolMessage;
          try {
            data = JSON.parse(message.toString());
          } catch (e: unknown) {
            console.error(e);
            return;
          }

          this.realtimeQueue.enqueue(async () => {
            await deferredValue.get();
            const modifiedMessage = await this.applyRealtimeMiddlewares(data, 'outgoing');
            if (modifiedMessage) {
              targetWs.send(JSON.stringify(modifiedMessage));
            }
          });
        });

        targetWs.on('message', (message) => {
          let data: ProtocolMessage;
          try {
            data = JSON.parse(message.toString());
          } catch (e: unknown) {
            console.error(e);
            return;
          }

          if (data.action === 4) {
            this.connectionIdToWs.set(data.connectionId!!, ws);
          }

          this.realtimeQueue.enqueue(async () => {
            const modifiedMessage = await this.applyRealtimeMiddlewares(data, 'incoming');
            if (modifiedMessage) {
              ws.send(JSON.stringify(modifiedMessage));
            }
          });
        });

        ws.on('close', () => {
          targetWs.close();
          this.closeWs(ws);
        });
        targetWs.on('close', () => this.closeWs(ws));
      });
    });
  }

  async observeNextRequest<T = any>(filter?: (req: Request<T>) => boolean): Promise<Request<T>> {
    const deferredValue = CompletableDeferred<Request<T>>();
    const unregister = this.registerRestMiddleware(async (req, res) => {
      if (!filter || filter(req)) deferredValue.complete(req);
      return res;
    });
    const request: Request<T>  = await deferredValue.get();
    unregister();
    return request;
  }

  async observeNextOutgoingProtocolMessage(filter?: (msg: ProtocolMessage) => boolean): Promise<ProtocolMessage> {
    const deferredValue = CompletableDeferred<ProtocolMessage>();
    const unregister = this.registerRealtimeMiddleware(async (msg, messageType) => {
      if (messageType === 'outgoing' && (!filter || filter(msg))) deferredValue.complete(msg);
      return { command: 'keep' };
    });
    const message: ProtocolMessage = await deferredValue.get();
    unregister();
    return message;
  }

  async observeNextResponse<T = any>(filter?: (res: Response<T>) => boolean): Promise<Response<T>> {
    const deferredValue = CompletableDeferred<Response<T>>();
    const unregister = this.registerRestMiddleware(async (_, res) => {
      if (filter && !filter(res)) return res;
      deferredValue.complete(res);
      return res;
    });

    const response: Response<T>  = await deferredValue.get();
    unregister();
    return response;
  }

  async observeNextIncomingProtocolMessage(filter?: (msg: ProtocolMessage) => boolean): Promise<ProtocolMessage> {
    const deferredValue = CompletableDeferred<ProtocolMessage>();
    const unregister = this.registerRealtimeMiddleware(async (msg, messageType) => {
      if (messageType === 'incoming' && (!filter || filter(msg))) deferredValue.complete(msg);
      return { command: 'keep' };
    });
    const message: ProtocolMessage = await deferredValue.get();
    unregister();
    return message;
  }

  pauseAllConnections(): () => void {
    let resume: () => void;
    const unregister = this.registerRealtimeMiddleware(async (_, __) => {
      await new Promise<void>((resolve) => (resume = resolve));
      return { command: 'keep' };
    });
    return () => {
      resume();
      unregister();
    };
  }

  registerRestMiddleware(middleware: RestMiddleware): () => void {
    this.restMiddlewares.push(middleware);

    return () => {
      const index = this.restMiddlewares.indexOf(middleware);
      if (index !== -1) {
        this.restMiddlewares.splice(index, 1);
      }
    };
  }

  registerRealtimeMiddleware(middleware: RealtimeMiddleware): () => void {
    this.realtimeMiddlewares.push(middleware);

    return () => {
      const index = this.realtimeMiddlewares.indexOf(middleware);
      if (index !== -1) {
        this.realtimeMiddlewares.splice(index, 1);
      }
    };
  }

  get options(): Ably.ClientOptions {
    if (!this.serverIsRunning) throw Error('You need to connect to the server before getting ');

    return {
      tls: false,
      port: this.port,
      realtimeHost: 'localhost',
      restHost: 'localhost',
    };
  }

  public async start() {
    return new Promise<void>((resolve) => {
      this.server.listen(this.port, () => {
        this.serverIsRunning = true;
        resolve();
      });
    });
  }

  public async replaceNextIncomingProtocolMessage(replacement: ProtocolMessage, filter?: (msg: ProtocolMessage) => boolean): Promise<void> {
    const deferredValue = CompletableDeferred<void>();
    const unregister = this.registerRealtimeMiddleware(async (msg, messageType) => {
      if (messageType === 'incoming' && (!filter || filter(msg))) {
        deferredValue.complete();
        return { command: 'replace', replacement };
      } else {
        return { command: 'keep' };
      }
    });
    await deferredValue.get();
    unregister();
  }

  public injectProtocolMessage(connectionId: string, msg: ProtocolMessage) {
    this.connectionIdToWs.get(connectionId)?.send(JSON.stringify(msg));
  }

  public dropConnection(connectionId: string) {
    this.connectionIdToWs.get(connectionId)?.close();
  }

  private get targetRestUrl(): string {
    const { restHost, tls, port, tlsPort } = this.realtimeClientOptions;
    return `${tls ? 'https' : 'http'}://${restHost}:${tls ? tlsPort : port}`;
  }

  private get targetRealtimeUrl(): string {
    const { realtimeHost, tls, port, tlsPort } = this.realtimeClientOptions;
    return `${tls ? 'wss' : 'ws'}://${realtimeHost}:${tls ? tlsPort : port}`;
  }

  private async applyRestMiddlewares(req: Request, res: Response): Promise<Response> {
    let resultResponse = res;
    for (const middleware of this.restMiddlewares) {
      resultResponse = await middleware(req, resultResponse);
    }
    return resultResponse;
  }

  private async applyRealtimeMiddlewares(
    msg: ProtocolMessage,
    messageType: MessageType,
  ): Promise<ProtocolMessage | undefined> {
    let resultMessage = msg;
    for (const middleware of this.realtimeMiddlewares) {
      const command = await middleware(resultMessage, messageType);
      switch (command.command) {
        case 'drop':
          return undefined;
        case 'replace':
          resultMessage = command.replacement!!;
          break;
        case 'keep':
          break;
      }
    }
    return resultMessage;
  }

  private closeWs(ws: WebSocket) {
    ws.close();
    this.connectionIdToWs.forEach((value, key) => {
      if (ws === value) this.connectionIdToWs.delete(key)
    });
  }
}

export function createInterceptingProxy(realtimeClientOptions: Ably.ClientOptions): InterceptingProxy {
  return new DefaultInterceptingProxy({
    tls: true,
    tlsPort: 443,
    port: 80,
    realtimeHost: 'sandbox-realtime.ably.io',
    restHost: 'sandbox-rest.ably.io',
    ...realtimeClientOptions,
  });
}
