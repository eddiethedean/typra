# Security threat model

This document defines Typra’s security posture as a **local, embedded** database library and the expectations we place on the codebase.

## Scope and non-goals

- **In scope**
  - Malicious or corrupted `.typra` files opened by the engine.
  - Untrusted input values passed through public APIs (Rust and Python).
  - Denial-of-service via pathological inputs (CPU, memory, disk growth).
- **Out of scope (for now)**
  - Network server exposure (Typra is not a DB server).
  - Multi-tenant isolation / sandboxing beyond normal OS process boundaries.
  - Cryptographic confidentiality guarantees (encryption-at-rest is a future consideration, not a current guarantee).

## Attacker model

Assume an attacker can provide:

- A crafted `.typra` file with arbitrary bytes (including truncated, torn-write, or checksum-colliding attempts).
- SQL text for the supported DB-API `SELECT` subset (Python), including adversarial whitespace and parameter edge cases.
- Arbitrary JSON schema descriptors in Python (`fields_json`, `indexes_json`) and arbitrary row values.

Assume the attacker **cannot**:

- Execute arbitrary code inside the process except through Typra’s bugs.
- Bypass OS permissions (Typra has no elevated privileges).

## Security invariants (must hold)

- **No `unsafe`**: the workspace forbids `unsafe` (`[workspace.lints.rust] unsafe_code = "forbid"`).
- **No panics from untrusted input**: decoder and parser failures should return structured errors, not panic.
- **Deterministic corruption handling**: checksums and decode failures yield deterministic, documented errors.
- **Recovery correctness**: in `AutoTruncate`, the engine must only recover to a prefix that preserves durable invariants; in `Strict`, it must fail fast.
- **Ephemeral spill isolation**: `Temp` segments are ignored by replay and must never influence durable state after reopen.

## Primary risk areas

- **File-format decode surfaces**: header, superblocks, segment headers, and payload decoders (catalog/record/index/checkpoint).
- **Replay logic**: transaction framing and checkpoint-assisted replay must not produce inconsistent in-memory state.
- **Planner/executor**: must not allocate unbounded memory for supported operator shapes; spill paths should engage as intended.
- **Python bindings**: conversions between Python values ↔ `RowValue` must validate types and avoid panic paths.

## Mitigations in the repo

- **Fuzzing**: `cargo-fuzz` targets exist under `fuzz/` for decode/replay surfaces.
- **Property/invariant tests**: snapshot roundtrips and other invariants are validated via `proptest`.
- **Coverage + doc verification**: CI runs `scripts/verify-doc-examples.sh` to prevent doc drift in supported user workflows.

## Operational guidance

- Treat `.typra` files as **untrusted input** when sourced externally.
- Prefer `RecoveryMode::Strict` when you need fail-fast behavior (e.g. automated pipelines).
- Use `AutoTruncate` when best-effort salvage is preferred and truncation is acceptable.

