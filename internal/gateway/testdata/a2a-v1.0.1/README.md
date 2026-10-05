# A2A 1.0 fixtures

Reference: https://github.com/a2aproject/A2A/releases/tag/v1.0.1

Pinned commit: `3303592588e388e62e0f69f701af531d2f4e3991`.
Wire version: `1.0`; binding: JSON-RPC over HTTP with SSE.

`a2a.proto` is the unmodified authoritative schema downloaded from
https://raw.githubusercontent.com/a2aproject/A2A/v1.0.1/specification/a2a.proto.
SHA-256: `e195bf96ab630c69797851970203e1b2b6b19528f2e9803b7d904b91a5104016`.
Its original Apache 2.0 license header is retained. The JSON fixtures are
locally authored examples derived from that schema, not SDK-generated captures.
They exercise SendMessageRequest/Response, the content oneof, ProtoJSON enums,
opaque business identifiers and integer precision. Test assertions also cover
stream envelopes, Card authentication and the other selected RPC methods.
These fixtures do not constitute external SDK/Hermes interoperability validation.
