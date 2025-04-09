# Local HTTP & WebSocket Proxy for SDK Testing

During SDK testing, it’s often useful to simulate real-world conditions or inspect communication between a client and a backend service. This lightweight local proxy helps developers spy on and inject both HTTP and WebSocket traffic for more effective debugging, experimentation, and test automation.

## ✨ Core Features

### 🔍 HTTP Inspection

- Spy on Outgoing HTTP Requests
- Modify Incoming HTTP Responses

### 🔍 WebSocket Inspection

- Spy on Outgoing WebSocket Messages
- Modify Incoming WebSocket Messages
- Inject WebSocket Messages

### 📦 Use Cases

- Validate SDK behavior against specific message sequences.
- Simulate dropped or malformed responses.
- Replay server messages for automated or regression testing.
- Monitor raw protocol-level activity to catch subtle bugs or race conditions.

### 🚀 Getting Started

Install npm dependency:

```bash
npm install --save-dev @ably-labs/local-proxy
```

### 🧪 Basic Usage

```ts
import { createInterceptingProxy } from '@ably-labs/local-proxy';

// 1. Create and start the proxy
const proxy = createInterceptingProxy({
  // options here realtimeHost, restHost
});
await proxy.start();

// 2. Configure your SDK to use proxy.options
const client = new Ably.Realtime({
  ...proxy.options
  // aditonal option
});

// 3. Observe an outgoing HTTP request
const request = await proxy.observeNextRequest(req =>
  req.url.includes('/channels')
);

// 4. Observe an incoming protocol message
proxy.observeNextIncomingProtocolMessage(msg =>
  msg.action === 15 // Presence message
)

// 5. Inject a fake message into the client
proxy.injectProtocolMessage(client.connection.id, {
    action: 9, // Example: SYNC
    channel: 'room:test',
    connectionId: client.connection.id,
    msgSerial: 0,
    connectionSerial: -1,
    data: { custom: 'data' },
});
```

### 🧩 Replace a Server Message

You can also simulate faulty server responses:

```ts
proxy.replaceNextIncomingProtocolMessage(
  {
    action: 9, // Fake SYNC
    channel: 'room:test',
    data: [],
  },
  msg => msg.action === 9 // Replace only SYNC messages
);
```


### 🔌 Drop or Pause a Connection

```ts
// Drop connection by ID (force disconnect)
proxy.dropConnection(client.connection.id);

// Pause and resume connection manually
const resume = proxy.pauseConnection();
// simulate a pause...
setTimeout(() => resume(), 5000);
```


### 🔧 Register Middleware

For more advanced use cases, register middlewares to continuously inspect or modify traffic:

```ts
const unregister = proxy.registerRestMiddleware(req => {
  if (req.url.includes('/channels')) {
    console.log('Intercepted REST request:', req);
    // You can modify headers, body, or response here
  }
});
// Later...
unregister();
```
