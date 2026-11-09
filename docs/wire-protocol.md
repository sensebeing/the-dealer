# Wire Protocol & Cryptographic Network Rendezvous

`the-dealer` separates **Cryptographic Network Isolation & Control Frames** from **Content-Addressed Bulk Blob Streams**, achieving sub-millisecond LAN delivery and low-latency 1,000-node mesh broadcast without a central server.

---

## 1. Cryptographic Network Derivation (`networkId + passphrase`)

A collaboration network (`Network`, e.g., `sensebeing` or `ben-home`) is uniquely identified by `(networkId, passphrase)`. The passphrase is never transmitted over the wire. Every node mounting `networkId` derives deterministic cryptographic keys using `HKDF-SHA256`:

```
master_ikm = SHA256("the-dealer:network:v1:" || networkId || ":" || passphrase)
swarmTopicId        = HKDF-Expand(master_ikm, "the-dealer:swarm-topic:v1", 32)   // 32-byte hex topic
rendezvousSeed      = HKDF-Expand(master_ikm, "the-dealer:rendezvous-ed25519:v1", 32)
networkSealedKey    = HKDF-Expand(master_ikm, "the-dealer:aead-sealed-key:v1", 32)
```

- **`swarmTopicId`**: Used as the opaque discovery fingerprint on LAN mDNS beacons and as the `iroh-gossip` Topic ID. External observers on the LAN cannot infer the human-readable `networkId` from the beacon.
- **`networkSealedKey`**: Used with `AES-256-GCM` / `ChaCha20-Poly1305` to seal all Gossip frames (`NodeCard` presence announcements and topic broadcasts) and to authenticate direct QUIC/TCP peer handshakes within that network. Nodes without the passphrase cannot decrypt `NodeCard` broadcasts or inject frames.

---

## 2. Wire Frame Format (`SealedNetworkFrame`)

Every packet exchanged within a mounted `Network` (over Gossip or Direct Unicast/Multicast stream) is wrapped in a `SealedNetworkFrame`:

```json
{
  "protocol": "the-dealer-wire/v1",
  "swarmTopicId": "a7f902...",
  "senderNodeId": "ed25519:3b8c...",
  "nonce": "base64-12-bytes",
  "ciphertext": "base64-aead-encrypted-payload",
  "authTag": "base64-16-bytes"
}
```

Once unsealed with `networkSealedKey`, the inner payload is one of:
1. **`peer.card`**: Signed `NodeCard` presence announcement (monotonic `revision`, `identity`, `admissionMode`, `capabilities`, `runtimeState`, `signature`).
2. **`message.envelope`**: Signed `MessageEnvelope` (`id`, `networkId`, `traceId`, `replyToId`, `idempotencyKey`, `from`, `routing`, `kind`, `priority`, `subject`, `body`, `payload`, `attachments`, `createdAt`, `signature`).
3. **`message.receipt`**: End-to-end `DeliveryReceipt` (`messageId`, `networkId`, `fromNodeId`, `stage`: `received | pending_approval | queued | dispatching | dispatched | settled | rejected | failed`, `detail`, `updatedAt`, `signature`).
4. **`blob.request` / `blob.chunk`**: Content-addressed chunk transfer verified against `blake3Hash` / `sha256` digest.
