# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in ANVIL, please report it privately. Do not open a public issue.

Send reports to: `security@example.com`

Include:
- Type of issue (e.g. buffer overflow, authentication bypass, assertion failure via network input)
- Reproducing steps or proof-of-concept
- Affected crates and versions
- Impact assessment

You will receive an acknowledgment within 24 hours and regular status updates until a patch is issued.

## Security Doctrine

ANVIL follows a strict security doctrine enforced by architecture:

1. **Secure by default**: Mutual TLS 1.3 is required for peers, clients, and administrative endpoints. Plaintext listeners exist only behind an explicit insecure flag refused in release builds.
2. **Authenticate before allocating**: Frame headers and credentials are authenticated prior to reserving memory buffers.
3. **Bounded inputs**: Every dynamic length is bounded before memory reservation. Every decoder is fuzzed.
4. **Least privilege**: Strict role separation (peer, client-read, client-write, admin) tied to certificate identities.
5. **No secret leakage**: Zeroize sensitive key material on drop. No credentials or user data in logs.
6. **Supply chain hygiene**: Pinned dependency graph, lockfile verified, zero unsafe Rust (`#![forbid(unsafe_code)]`), and automated cargo-deny / cargo-audit verification on every commit.

Full security rationale is tracked in `docs/security/threat-model.md`.
