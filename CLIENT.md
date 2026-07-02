# NervesHub Client Protocol Reference

This document describes the NervesHub protocol as implemented by `nerves_hub_link`,
covering only the parts relevant to our minimal Rust client.

## Connection

The client connects via WebSocket to a NervesHub server. Two authentication methods
are supported. They use different endpoints.

- **mTLS**: `wss://{host}/socket/websocket`
- **Shared Secret**: `wss://{host}/device-socket/websocket`

They both use the same Phoenix Channels protocol after the connection is established.

## Phoenix Channels Protocol

All messages are JSON arrays with 5 elements:

```
[join_ref, ref, topic, event, payload]
```

- `join_ref`: Set during join, null for server pushes
- `ref`: Incrementing message reference (string of integer), null for server broadcasts
- `topic`: Channel topic string
- `event`: Event name string
- `payload`: JSON object

### Lifecycle Events

**Join** (client -> server):
```json
["1", "1", "device:SERIAL", "phx_join", { ...metadata }]
```

**Join Reply** (server -> client):
```json
["1", "1", "device:SERIAL", "phx_reply", {"status": "ok", "response": {}}]
```

**Heartbeat** (client -> server):
```json
[null, "2", "phoenix", "heartbeat", {}]
```

**Heartbeat Reply** (server -> client):
```json
[null, "2", "phoenix", "phx_reply", {"status": "ok", "response": {}}]
```

**Server Push** (server -> client):
```json
[null, null, "device:SERIAL", "update", { ...payload }]
```

**Client Push** (client -> server):
```json
["1", "3", "device:SERIAL", "fwup_progress", { ...payload }]
```

**Error Reply**:
```json
["1", "1", "device:SERIAL", "phx_reply", {"status": "error", "response": {"reason": "..."}}]
```

**Close**:
```json
[null, null, "device:SERIAL", "phx_close", {}]
```

## Authentication

### mTLS

The device presents its client certificate during the TLS handshake. The server
validates the certificate against its CA chain and extracts the device identity
from the certificate.

Configuration needed:
- Device certificate (PEM or DER)
- Device private key (PEM or DER, or OpenSSL engine reference)
- CA certificate chain (PEM)
- Server CA for verification

### Shared Secret

The device connects with HMAC-based authentication headers on the WebSocket upgrade request.

**Headers:**
```
x-nh-alg:       NH1-HMAC-sha256-1000-32
x-nh-key:       <key_identifier>
x-nh-time:      <unix_timestamp_seconds>
x-nh-signature: <hmac_signature>
```

**Algorithm string format:** `NH1-HMAC-{digest}-{iterations}-{key_length}`

**Signature generation:**
1. Build salt string:
   ```
   NH1:device-socket:shared-secret:connect\n\nx-nh-alg={alg}\nx-nh-key={key}\nx-nh-time={timestamp}
   ```
2. Derive signing key using PBKDF2:
   - Secret: the shared secret
   - Salt: the salt string above
   - Iterations: from algorithm (e.g. 1000)
   - Key length: from algorithm (e.g. 32)
   - Digest: from algorithm (e.g. sha256)
3. HMAC-sign the device identifier (serial number) using the derived key
4. Base64-encode the signature

The server verifies the signature and checks that `x-nh-time` is within 90 seconds.

## Device Channel

### Topic

`device:{identifier}` where identifier is the device serial number.

### Join Payload

```json
{
  "device_api_version": "2.3.0",
  "fwup_version": "1.10.1",
  "nerves_fw_uuid": "<current-firmware-uuid>",
  "nerves_fw_version": "<current-firmware-version>",
  "nerves_fw_platform": "<platform>",
  "nerves_fw_architecture": "<architecture>",
  "nerves_fw_product": "<product-name>"
}
```

### Server Events (server -> client)

#### `update` - Firmware Update Available

```json
{
  "firmware_url": "https://s3.example.com/firmware.fw?signed-params",
  "firmware_meta": {
    "uuid": "new-firmware-uuid",
    "version": "1.1.0",
    "platform": "rpi4",
    "architecture": "arm",
    "product": "my-product"
  }
}
```

#### `reboot` - Reboot Command

```json
{}
```

### Client Events (client -> server)

#### `fwup_progress` - Download/Apply Progress

```json
{
  "stage": "downloading",
  "value": 50
}
```

#### `status_update` - Update Status

```json
{
  "status": "completed"
}
```

Current statuses: `"received"`, `"started"`, `"completed"`, `"failed"`, `"ignored"`, `"rescheduled"`.

`failed`, `ignored`, and `rescheduled` may include extra context such as `reason` or `delay_for`.

#### `rebooting` - Reboot Acknowledgment

```json
{}
```

## Firmware Update Flow

1. Server pushes `update` event with `firmware_url` and `firmware_meta`
2. Client downloads firmware from the pre-signed URL via HTTPS
3. Client reports download progress via `fwup_progress` events with `stage: "downloading"` and `value: 0-100`
4. Client writes downloaded firmware to a temporary file and renames it after flush
5. The default daemon installer runs `fwup` CLI to apply the firmware:
   ```
   fwup -a -d /dev/mmcblk0 -i /tmp/link/firmware-UUID.fw -t upgrade
   ```
6. Client reports final apply progress with `stage: "updating"` and `value: 100`
7. Client reports completion via `status_update`
8. Client may reboot (or wait for server `reboot` command)

## Heartbeat

The client must send heartbeat messages at regular intervals (default: 30 seconds).
If the server doesn't receive a heartbeat within the timeout window, it considers
the device disconnected.

## Extensions And Health

The server may ask the device for extension support with `extensions:get` on the
device topic. The client responds by joining the `extensions` topic with available
extension versions:

```json
{"health": "0.0.1"}
```

Health reports are sent on the `extensions` topic using `health:report`:

```json
{
  "value": {
    "timestamp": "2026-07-02T21:17:26Z",
    "metadata": {},
    "alarms": {},
    "metrics": {},
    "checks": {},
    "connectivity": {}
  }
}
```

## Reconnection

On disconnect, the client should reconnect with exponential backoff:
- Initial delay: 1 second
- Maximum delay: 60 seconds
- Jitter: up to 50% of delay

For Shared Secret auth, the signature must be regenerated on each connection attempt
(the timestamp changes).

## Device Serial Number

The device serial number is used for shared-secret signed data. The library does not
read platform stores directly; callers provide the serial and firmware metadata through
static configuration, direct `DeviceInfo`, or a `DeviceInfoProvider`.
