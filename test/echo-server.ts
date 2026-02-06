import http from 'http';
import { WebSocketServer } from 'ws';

export async function startEchoServer(port = 8080): Promise<http.Server> {
  const echoServer = http.createServer((req, res) => {
    const bodyChunks: any[] = [];
    req.on('data', (chunk) => bodyChunks.push(chunk));
    req.on('end', () => {
      const requestData = Buffer.concat(bodyChunks).toString();
      res.writeHead(200);
      res.end(requestData);
    });
  });

  const wss = new WebSocketServer({ server: echoServer, path: '/' });

  wss.on('connection', (ws) => {
    ws.on('message', (message) => {
      ws.send(message);
    });
  });

  await new Promise<void>((resolve) => {
    echoServer.listen(port, () => resolve());
  });

  return echoServer;
}

export async function stopEchoServer(server: http.Server) {
  return new Promise<void>((resolve) => server.close(() => resolve()));
}
