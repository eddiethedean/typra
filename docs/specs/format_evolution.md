# Format evolution (1.x)

**Audience:** engine contributors and release maintainers

Rules for changing the `.typra` on-disk format during the **1.x** release line without breaking files users ship with their apps.

## Promise (1.x)

For **`FORMAT_MAJOR = 0`** and any **1.y.z** engine release:

1. **Read compatibility** — Typra **1.y** can open and replay any `.typra` file written by **1.0+** (format minors **3–6**), including:
   - Catalog payloads **v1–v4**
   - Record payloads **v1, v2, v3**
   - Index payloads **v1–v2**
   - Unframed segment logs (minor ≤ 5) after header upgrade to minor 6
2. **Semantic stability** — Replay of an unchanged file prefix yields the same logical rows, indexes, and catalog state as when the file was written (subject to [Recovery modes](../reference/compatibility.md#recovery-modes-contract)).
3. **Write compatibility** — Opening an older file and performing supported operations **must not destroy** existing committed data. Lazy header upgrade (minor → 6) is allowed; whole-file silent rewrite is not.

Breaking any of the above requires **`FORMAT_MAJOR` bump** and Typra **2.0** (new SemVer major).

## Allowed in 1.x (additive)

| Change | Requirement |
|--------|-------------|
| New **optional** segment type | Document as ignorable or ephemeral; old readers must refuse or ignore per [compatibility](../reference/compatibility.md) |
| New **record/catalog/index payload version** | Old versions remain readable; new writes may prefer newer version |
| New **format minor** (e.g. 7) | Old minors **3–6** remain readable; document read/write matrix in [Compatibility](../reference/compatibility.md) |
| Stricter **validation** on write | Must not change replay of existing segments |
| New engine features (SQL, planner, compaction) | Must not alter decode of existing segment types |

## Forbidden in 1.x without major bump

- Changing meaning of existing segment types **1–8** (Schema, Record, Index, Manifest, Checkpoint, TxnBegin/Commit/Abort)
- Reusing or renumbering segment type IDs
- Changing checksum algorithm for existing headers without dual-read support
- Removing replay support for catalog v1–v4 or record v1–v3
- Changing last-write-wins replay rules for `(collection_id, pk)`
- Shrinking maximum path lengths or type tags in a way that rejects previously valid files

## Release checklist (format touch)

When a PR changes `crates/typra-core/src/{file_format,superblock,segments,catalog,record,index,txn,checkpoint,publish,db/replay,db/recover}.rs`:

1. Update [Compatibility](../reference/compatibility.md) if read/write policy changes.
2. Update wire specs under [`specs/`](index.md) if layouts change.
3. Run `make test-format-compat` (or full `make check-full`).
4. If bytes on disk change intentionally, run `scripts/generate-format-fixtures.sh` and commit updated fixtures.
5. Add a **CHANGELOG** entry under the target 1.x version describing user-visible format impact.

## Tests

| Test | Enforces |
|------|----------|
| `crates/typra-core/tests/integration/format_back_compat_1x.rs` | 1.0-shaped files: read, append, reopen |
| `crates/typra-core/tests/unit/src_db_replay_tests.rs` | Minor 6 replay of unframed legacy log |
| `crates/typra-core/tests/integration/record_payload_v1_v2_delete_edges.rs` | Record v1/v2 decode |
| `crates/typra-core/tests/integration/schema_paths_multi_segment.rs` | Record v3 + nested paths |

Committed golden files: `crates/typra-core/tests/fixtures/format/` (see README there).
