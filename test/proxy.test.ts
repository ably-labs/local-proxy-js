import { beforeAll, describe, expect, it } from 'vitest';
import WebSocket from 'ws';
import { createInterceptingProxy, type InterceptingProxy } from '../src';
import { delay, openConnection, runAndObserve, waitForConnectionClose, waitForNextReceivedMessage } from './utils';
import { startEchoServer, stopEchoServer } from './echo-server';

describe('Local proxy tests', () => {
  let proxy: InterceptingProxy;

  beforeAll(async () => {
    const port = 8080;
    const server = await startEchoServer(port);
    proxy = createInterceptingProxy({
      realtimeHost: 'localhost',
      restHost: 'localhost',
      port,
      tls: false,
    });
    await proxy.start();
    return async () => stopEchoServer(server);
  });

  it('should modify incoming websocket messages from server', async () => {
    const testWs = new WebSocket(`ws://localhost:${proxy.options.port}`);

    await openConnection(testWs);

    proxy.replaceNextIncomingProtocolMessage({ action: 3 });

    await runAndObserve(
      async () => testWs.send(JSON.stringify({ action: 1 })),
      async () => {
        const message = await proxy.observeNextIncomingProtocolMessage();
        const rawWebsocketMessage = await waitForNextReceivedMessage(testWs);
        expect(message).toEqual({ action: 3 });
        expect(rawWebsocketMessage.toString()).toBe('{"action":3}');
      },
    );

    testWs.close();
  });

  it('should spy on outgoing websocket messages from server', async () => {
    const testWs = new WebSocket(`ws://localhost:${proxy.options.port}`);

    await openConnection(testWs);

    await runAndObserve(
      async () => testWs.send(JSON.stringify({ action: 0 })),
      async () => {
        const message = await proxy.observeNextOutgoingProtocolMessage();
        expect(message).toEqual({ action: 0 });
      },
    );

    testWs.close();
  });

  it('should spy on incoming websocket messages from server', async () => {
    const testWs = new WebSocket(`ws://localhost:${proxy.options.port}`);

    await openConnection(testWs);

    await runAndObserve(
      async () => testWs.send(JSON.stringify({ action: 0 })),
      async () => {
        const message = await proxy.observeNextIncomingProtocolMessage();
        expect(message).toEqual({ action: 0 });
      },
    );

    testWs.close();
  });

  it('should inject websocket messages', async () => {
    const testWs = new WebSocket(`ws://localhost:${proxy.options.port}`);

    // we simulate connectionId that should be available in RealtimeClient
    const connectionId = 'connectionId';

    await openConnection(testWs);
    testWs.send(JSON.stringify({ action: 4, connectionId }));

    await waitForNextReceivedMessage(testWs);

    await runAndObserve(
      async () => proxy.injectProtocolMessage(connectionId, { action: 5, channel: 'test-channel' }),
      async () => {
        const message = await waitForNextReceivedMessage(testWs);
        expect(message.toString()).toEqual(JSON.stringify({ action: 5, channel: 'test-channel' }));
      },
    );

    testWs.close();
  });

  it('should drop websocket connection', async () => {
    const testWs = new WebSocket(`ws://localhost:${proxy.options.port}`);

    // we simulate connectionId that should be available in RealtimeClient
    const connectionId = 'connectionId';

    await openConnection(testWs);
    testWs.send(JSON.stringify({ action: 4, connectionId }));
    await waitForNextReceivedMessage(testWs);

    await runAndObserve(
      async () => proxy.dropConnection(connectionId),
      async () => {
        await waitForConnectionClose(testWs);
      },
    );

    testWs.close();
  });

  it('should observe http requests', async () => {
    await runAndObserve(
      async () => {
        await fetch(`http://localhost:${proxy.options.port}/foo/bar`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ foo: 'bar' }),
        });
      },
      async () => {
        const request = await proxy.observeNextRequest();
        expect(request.data).toEqual({ foo: 'bar' });
      },
    );
  });

  it('should modify http responses', async () => {
    const unregister = proxy.registerRestMiddleware(async (_, res) => {
      return {
        ...res,
        data: { bar: 'foo' },
      };
    });
    const result = await fetch(`http://localhost:${proxy.options.port}/foo/bar`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ foo: 'bar' }),
    });
    const response = await result.json();
    expect(response).toMatchObject({ bar: 'foo' });
    unregister();
  });

  it('should be able to pause connection', async () => {
    const testWs = new WebSocket(`ws://localhost:${proxy.options.port}`);

    // we simulate connectionId that should be available in RealtimeClient
    await openConnection(testWs);

    const failOnMessageListener = () => {
      expect.fail("We shouldn't receive any messages, while on pause");
    };

    testWs.on('message', failOnMessageListener);

    const resume = proxy.pauseAllConnections();
    testWs.send(JSON.stringify({ action: 0 }));
    await delay(1000)
    testWs.off('message', failOnMessageListener);

    runAndObserve(
      async () => {
        resume();
      },
      async () => {
        const rawWebsocketMessage = await waitForNextReceivedMessage(testWs);
        expect(rawWebsocketMessage.toString()).toBe('{"action":0}');
      }
    )

    testWs.close();
  });
});
