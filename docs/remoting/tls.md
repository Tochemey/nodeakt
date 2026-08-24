# TLS

TLS is per-system configuration, the `tls` block of `RemoteOptions` (`TlsOptions`): the node listens over TLS and dials every peer over TLS. Everything above the sockets — the protocol, the messages, the semantics — is byte-identical to [plaintext remoting](index.md).

```ts
const system = new ActorSystem("orders", {
  remote: {
    host: "10.0.0.5",
    port: 5100,
    tls: {
      cert: "/etc/nodeakt/node.pem",   // PEM contents or a file path
      key: "/etc/nodeakt/node.key",
      ca: "/etc/nodeakt/ca.pem",
    },
  },
});
```

- **One block, both roles.** Every node listens and dials, so the same material serves both: the listener presents `cert` and `key`, the dialer verifies peers against `ca` and expects the dialed host to match the certificate. Without `ca`, the runtime's default trust store verifies, which refuses the self-signed material private clusters typically run on.
- **All or nothing per system.** A TLS node accepts only TLS and dials only TLS. A mixed pair, one node encrypted and one not, is a misconfiguration: the connection fails its handshake and surfaces as the dial or accept failure it is, never as a silent plaintext fallback. Run every node of a cluster in the same mode.
- **Mutual TLS.** Set `requestCert: true` and the node demands and verifies a client certificate on every accepted connection. Peers present the `cert` from their own block, so a cluster on one CA needs nothing extra. A verified peer certificate is an identity, not an authorization: nothing checks what a verified peer may do.
- **Certificates are yours.** Provide PEM contents or the path of a PEM file; paths are read once at `start()`, and unreadable or malformed material rejects the start, so a node meant to be encrypted never comes up plaintext. Rotation is a restart. Nothing generates, renews, or watches certificates.
- **The certificate must name the advertised host.** A dialer verifies the peer's identity against the `host` it dialed, so each node's certificate must list its advertised `host` in the subject alternative names: a `DNS:` entry for a hostname, an `IP:` entry for an address. A cluster addressed by IP therefore needs `IP:` SANs; without a match, dials fail with `ERR_TLS_CERT_ALTNAME_INVALID` even though encryption itself succeeded.

> [!WARNING]
> TLS encrypts and verifies certificates. It does not authorize what a verified peer may do, and a sender's identity inside an envelope is still self-declared. Do not expose a remoting port to an untrusted network.

## Performance

Enabling TLS **hinders performance**. Encryption is paid per byte and the certificate handshake per connection: on loopback benchmarks, fire-and-forget throughput drops a few percent and each sequential ask round trip gains a few milliseconds; real networks pay more.

The recommended deployment is to run the nodes inside a VPC, private network, or otherwise secured environment, and reach for TLS when traffic must cross a boundary that environment does not cover, or when policy demands encryption in transit regardless.
