# Security Policy

## Supported versions

streamcontext is pre-1.0 and ships from `main`. Security fixes land on the
latest released minor; older alpha cuts are not maintained.

## Threat model

streamcontext assumes a local or trusted-host deployment. The ingestion gateway
and catalog refresher run on infrastructure the operator controls, and the MCP
server runs alongside the operator's own agent host over stdio or loopback SSE.
Multi-tenant exposure, internet-facing MCP servers, and per-caller authentication
are explicitly out of scope today, though the MCP server ships an `authorize`
hook (`build_server(authorize=...)`) as the seam for plugging in a real check.

The full threat model, what each of the three processes does and does not
protect, and the recommended production-adjacent configuration live in
[`docs/security.md`](docs/security.md).

## Audit trail

Each release is preceded by a security audit using a Block / Fix-next / Defer /
Resolved scheme:

- [`docs/audit-v0.1.md`](docs/audit-v0.1.md) — ingestion gateway.
- [`docs/audit-v0.2.md`](docs/audit-v0.2.md) — MCP server.
- [`docs/audit-v0.3.md`](docs/audit-v0.3.md) — semantic catalog.

## Reporting a vulnerability

Please report security issues privately rather than in a public issue. Open a
private GitHub security advisory:

<https://github.com/jagguvarma15/streamcontext/security/advisories/new>

This matches the process documented in [`docs/security.md`](docs/security.md).
Public issues remain the right place for non-sensitive bugs and feature
requests.
