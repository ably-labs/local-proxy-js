import type WebSocket from 'ws';

export async function runAndObserve(runner: () => Promise<void>, observer: () => Promise<void>) {
  const observerPromise = observer();
  await runner();
  await observerPromise;
}

export async function openConnection(ws: WebSocket) {
  return new Promise<void>((resolve) => ws.once('open', () => resolve()));
}

export async function waitForNextReceivedMessage(ws: WebSocket) {
  return new Promise<WebSocket.RawData>((resolve) => {
    ws.once('message', (data) => {
      resolve(data);
    });
  });
}

export async function waitForConnectionClose(ws: WebSocket) {
  return new Promise<void>((resolve) => {
    ws.once('close', () => {
      resolve();
    });
  });
}

export function delay(timeoutMs: number) {
  return new Promise<void>((resolve) => {
    setTimeout(() => resolve(), timeoutMs);
  });
}
