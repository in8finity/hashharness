# system-model

Formal models of the hashharness integrity invariants. Two complementary artifacts: a bounded Alloy 6 model that exercises the full protocol (including adversaries), and an unbounded Dafny 4 proof of the two invariants that most need a "for *any* number of records" guarantee.

The models are the source of truth for *which* invariants the system claims to enforce; `reports/` cross-checks those claims against the production code and tests.

## Invariants modeled

| # | Invariant | Where |
|---|---|---|
| I1 | `text_sha256` is a unique storage id; no two distinct accepted records share it. | Alloy + Dafny |
| I2 | `record_sha256` is functional over `(text, predecessor)`; inter-item links pin the target's `record_sha256`. | Alloy |
| I3 | `created_at` is server-stamped; new records' timestamps strictly exceed every previously accepted record's. | Alloy + Dafny |
| I4 | For chain types, no two accepted records share the same predecessor (CAS on chain head). | Alloy |
| I5 | Every accepted record is bound to a schema version that exists in the schema chain; a loosen → write → tighten cycle leaves the loose schema permanently auditable. | Alloy |

## `system.als` — Alloy 6, bounded

Temporal model (Alloy 6 `var sig`s) of the full protocol surface: schema chain CAS, chain-head CAS on `create_item`, server-stamped time, schema binding. Hashes are modeled as opaque collision-free atoms.

What it produces:

- **Eight safety assertions** (`UniqueTextHash`, `RecordHashUnique`, `LinkPinsRecordIdentity`, `NoBackdateHonest`, `NoForkHonest`, `SchemaBindingSound`, `BindingPinsRecordAndSchema`, `SchemaBindingPermanent`) — all pass at scope `5 but 8 Step`.
- **Two causal-proof runs** (`BackdateBypassesIfStampingRelaxed`, `ForkBypassesIfCASRelaxed`) — go SAT, producing concrete counterexamples in which an adversarial transition that bypasses one defense violates the corresponding invariant. This proves each defense is causally necessary, not vacuously true.
- **Two faithfulness witnesses** (`HappyPathChain`, `SchemaSwapAuditTrail`) — go SAT, confirming the honest behaviors the model claims to permit are actually reachable.

Result: 8/8 assertions pass; 4/4 SAT runs find their witnesses.

## `system.dfy` — Dafny 4, unbounded

Proves I1 (text uniqueness) and I3 (server-stamped strict-monotone `created_at`) for *any* number of records and *any* interleaving of honest `CreateItem` calls. The Alloy proof is conditional on a bounded scope; this is the universal complement.

`CreateItemAdversarial(t)` is the adversarial method: it verifies *only* with the precondition `requires t >= h.now`. Removing that precondition causes Dafny to refuse the proof — the unbounded analogue of the Alloy `BackdateBypassesIfStampingRelaxed` run going SAT.

## `reports/`

- **`system-reconciliation.md`** — model ↔ code alignment, invariant by invariant.
- **`system-enforcement.md`** — model ↔ code ↔ test cross-check. Final verdict: **12/12 enforced**, every modeled invariant has both a code gate and a test (positive + negative) that exercises it. The audit closed one gap by adding `test_http_transport_create_item_rejects_caller_supplied_created_at`.

## Mapping to code

| Model element | Production code |
|---|---|
| `pred CreateItem` / `method CreateItem` | `src/hashharness/storage.py` `create_item` |
| `pred SetSchema` (CAS on `SchemaHead`) | `src/hashharness/storage.py` `set_schema` |
| `var sig ChainHead` + CAS in `CreateItem` | per-`(work_package_id, item_type)` head pointer in `storage.py` |
| `var sig SchemaHead` + CAS in `SetSchema` | schema head pointer (`data/schemas/HEAD` or `schema_head` row) |
| MCP boundary rejection of caller `created_at` | `src/hashharness/mcp_server.py` |

## Running the models

```bash
# Alloy 6: open system.als in the Alloy Analyzer and execute all checks/runs.
# Dafny 4:
dafny verify system.dfy
```
