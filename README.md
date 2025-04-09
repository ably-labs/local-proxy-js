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

🚀 Getting Started

Install npm dependency:

```bash
npm i -D @ably-labs/local-proxy
```




