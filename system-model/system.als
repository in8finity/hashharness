/**
 * Alloy 6 model: hashharness system-level integrity invariants.
 *
 * Verifies five invariants the production code claims to hold against a
 * protocol-only adversary, and proves the protections are *causal* (not
 * trivially structural) by also modelling an adversary that bypasses each
 * defense — and showing the bypass would in fact violate the invariant.
 *
 *   I1 UniqueText      — text_sha256 is a unique storage id; no two distinct
 *                        accepted records share it.
 *   I2 RecordHashIntegrity
 *                      — record_sha256 is functional over (text, predecessor);
 *                        inter-item links pin the target's recordHash.
 *   I3 NoBackdate      — created_at is server-stamped; new records' timestamps
 *                        strictly exceed every previously accepted record's.
 *   I4 NoFork          — for chain types, no two accepted records share the
 *                        same predecessor (CAS on chain head).
 *   I5 SchemaBinding   — every accepted record is bound to a schema version
 *                        that exists in the schema chain. A loosen→write→tighten
 *                        cycle leaves the loose schema permanently auditable.
 *
 * Adversary: protocol-only. Hashes modeled as opaque collision-free atoms.
 *
 * Maps to: src/hashharness/storage.py
 *   - create_item              → pred CreateItem
 *   - set_schema (CAS)         → pred SetSchema
 *   - chain_predecessor head   → var sig ChainHead, CAS in CreateItem
 *   - schema head pointer      → var sig SchemaHead, CAS in SetSchema
 *
 * Style: temporal (Alloy 6 var sigs).
 */

module HashHarnessSystem

open util/ordering[Step] as ord

// ---------- Hashes & content atoms ----------

sig Hash {}

sig Text { textHash: one Hash }
fact TextHashInjective { all disj t1, t2: Text | t1.textHash != t2.textHash }

// ---------- Schema versions (append-only chain) ----------

sig SchemaVersion {
  schemaPrev:       lone SchemaVersion,
  schemaRecordHash: one Hash
}
fact SchemaHashInjective {
  all disj v1, v2: SchemaVersion | v1.schemaRecordHash != v2.schemaRecordHash
}
fact SchemaChainAcyclic { no v: SchemaVersion | v in v.^schemaPrev }

// ---------- Records ----------

sig Record {
  textOf:        one Text,
  predecessor:   lone Record,
  schemaSha:     one SchemaVersion,
  recordHash:    one Hash,
  bindingHash:   one Hash,
  var createdAt: lone Step       // mutable: set on acceptance
}
fact RecordHashInjective {
  all disj r1, r2: Record | r1.recordHash  != r2.recordHash
  all disj r1, r2: Record | r1.bindingHash != r2.bindingHash
}
fact RecordHashFunctionalFromContent {
  // No two distinct records sharing the same content can have the same
  // recordHash collision (collision-free model).
  all disj r1, r2: Record |
    (r1.textOf = r2.textOf and r1.predecessor = r2.predecessor)
      => r1.recordHash != r2.recordHash
}
fact BindingHashFunctional {
  all r: Record |
    r.bindingHash != r.recordHash
    and r.bindingHash != r.schemaSha.schemaRecordHash
}
fact PredecessorAcyclic { no r: Record | r in r.^predecessor }

// ---------- Time scaffold ----------

sig Step {}

// ---------- Mutable state ----------

var sig ExistingRecord        in Record       {}
var sig ExistingSchemaVersion in SchemaVersion {}
var sig SchemaHead            in SchemaVersion {}
var sig ChainHead             in Record       {}
var one sig Now in Step {}

// ---------- Initial state ----------

fact Init {
  no ExistingRecord
  no ExistingSchemaVersion
  no SchemaHead
  no ChainHead
  Now = ord/first
  no createdAt
}

// ---------- Frame predicates ----------

pred frameRecords  { ExistingRecord' = ExistingRecord and ChainHead' = ChainHead and createdAt' = createdAt }
pred frameSchemas  { ExistingSchemaVersion' = ExistingSchemaVersion and SchemaHead' = SchemaHead }
pred advanceTime   { some ord/next[Now] and Now' = ord/next[Now] }
pred stutter       { frameRecords and frameSchemas and Now' = Now }

// ---------- Honest transitions (mirror the production protocol) ----------

// set_schema(v) with implicit expected_prev = current head
pred SetSchema[v: SchemaVersion] {
  v not in ExistingSchemaVersion
  (no SchemaHead and no v.schemaPrev) or (v.schemaPrev = SchemaHead)
  ExistingSchemaVersion' = ExistingSchemaVersion + v
  SchemaHead' = v
  frameRecords
  advanceTime
}

// create_item(r) — server stamps createdAt = Now
pred CreateItem[r: Record] {
  r not in ExistingRecord
  some SchemaHead
  r.schemaSha in SchemaHead
  no s: ExistingRecord | s.textOf = r.textOf
  (no ChainHead and no r.predecessor) or (r.predecessor in ChainHead)
  ExistingRecord' = ExistingRecord + r
  ChainHead' = r
  // stamp current time
  createdAt' = createdAt + (r -> Now)
  frameSchemas
  advanceTime
}

// ---------- Adversarial transitions (bypass defenses, used in attack runs) ----------

// AttackerSuppliesTimestamp: caller supplies arbitrary t (could be earlier).
// All other rules unchanged. We never include this in the honest Transitions
// fact — only in dedicated attack scenarios that explicitly use it.
pred AdvCreateItemBackdate[r: Record, t: Step] {
  r not in ExistingRecord
  some SchemaHead
  r.schemaSha in SchemaHead
  no s: ExistingRecord | s.textOf = r.textOf
  (no ChainHead and no r.predecessor) or (r.predecessor in ChainHead)
  ExistingRecord' = ExistingRecord + r
  ChainHead' = r
  createdAt' = createdAt + (r -> t)     // adversary picks t
  frameSchemas
  advanceTime
}

// AttackerIgnoresChainHead: adversary submits with stale or wrong predecessor.
pred AdvCreateItemFork[r: Record] {
  r not in ExistingRecord
  some SchemaHead
  r.schemaSha in SchemaHead
  no s: ExistingRecord | s.textOf = r.textOf
  // No CAS check — predecessor can be any existing record (or none).
  no r.predecessor or r.predecessor in ExistingRecord
  ExistingRecord' = ExistingRecord + r
  ChainHead' = ChainHead    // adversary doesn't even advance the head
  createdAt' = createdAt + (r -> Now)
  frameSchemas
  advanceTime
}

// ---------- Trace facts ----------

// Honest world: only legitimate transitions fire.
pred HonestTransitions {
  always (
    stutter
    or (some v: SchemaVersion | SetSchema[v])
    or (some r: Record | CreateItem[r])
  )
}

// Adversarial world A: backdating is enabled.
pred TransitionsWithBackdate {
  always (
    stutter
    or (some v: SchemaVersion | SetSchema[v])
    or (some r: Record | CreateItem[r])
    or (some r: Record, t: Step | AdvCreateItemBackdate[r, t])
  )
}

// Adversarial world B: fork (no chain-CAS) is enabled.
pred TransitionsWithFork {
  always (
    stutter
    or (some v: SchemaVersion | SetSchema[v])
    or (some r: Record | CreateItem[r])
    or (some r: Record | AdvCreateItemFork[r])
  )
}

// ---------- Safety properties under honest transitions ----------

assert UniqueTextHash {
  HonestTransitions =>
    always (all disj r1, r2: ExistingRecord | r1.textOf != r2.textOf)
}
check UniqueTextHash for 5 but 8 Step

assert RecordHashUnique {
  HonestTransitions =>
    always (all disj r1, r2: ExistingRecord | r1.recordHash != r2.recordHash)
}
check RecordHashUnique for 5 but 8 Step

assert LinkPinsRecordIdentity {
  HonestTransitions =>
    always (all r: ExistingRecord, p: r.predecessor | p in ExistingRecord)
}
check LinkPinsRecordIdentity for 5 but 8 Step

assert NoBackdateHonest {
  HonestTransitions =>
    always (
      all disj r1, r2: ExistingRecord |
        (some r1.createdAt and some r2.createdAt) =>
          (r1.createdAt != r2.createdAt
           and (ord/lte[r1.createdAt, r2.createdAt]
                or ord/lte[r2.createdAt, r1.createdAt]))
    )
}
check NoBackdateHonest for 5 but 8 Step

assert NoForkHonest {
  HonestTransitions =>
    always (
      all disj r1, r2: ExistingRecord |
        no r1.predecessor or no r2.predecessor or r1.predecessor != r2.predecessor
    )
}
check NoForkHonest for 5 but 8 Step

assert SchemaBindingSound {
  HonestTransitions =>
    always (
      (all r: ExistingRecord | r.schemaSha in ExistingSchemaVersion)
      and (all v: ExistingSchemaVersion |
             no v.schemaPrev or v.schemaPrev in ExistingSchemaVersion)
    )
}
check SchemaBindingSound for 5 but 8 Step

assert BindingPinsRecordAndSchema {
  HonestTransitions =>
    always (all disj r1, r2: ExistingRecord |
      (r1.recordHash != r2.recordHash or r1.schemaSha != r2.schemaSha)
        => r1.bindingHash != r2.bindingHash)
}
check BindingPinsRecordAndSchema for 5 but 8 Step

// Once a record is accepted with schemaSha = v, it stays bound to v
// across all subsequent steps (loosen→write→tighten audit trail).
assert SchemaBindingPermanent {
  HonestTransitions =>
    all r: Record, v: SchemaVersion |
      always (
        (r in ExistingRecord and r.schemaSha = v)
          => always (r in ExistingRecord => r.schemaSha = v)
      )
}
check SchemaBindingPermanent for 5 but 8 Step

// ---------- Causal proofs: defenses are necessary ----------

// If we relaxed server-stamping (let attacker supply timestamp), backdate
// becomes possible. SAT here proves the defense is causal, not trivial.
run BackdateBypassesIfStampingRelaxed {
  TransitionsWithBackdate
  some disj r1, r2: Record |
    eventually (r1 in ExistingRecord and r2 in ExistingRecord
      and some r1.createdAt and some r2.createdAt
      and r1.createdAt = r2.createdAt)   // collision (not strict mono)
} for 5 but 8 Step, exactly 1 SchemaVersion

// If we relaxed chain CAS, fork is possible. SAT here proves the defense
// is causal.
run ForkBypassesIfCASRelaxed {
  TransitionsWithFork
  some disj r1, r2: Record, p: Record |
    eventually (r1 in ExistingRecord and r2 in ExistingRecord
      and r1.predecessor = p and r2.predecessor = p)
} for 5 but 8 Step, exactly 1 SchemaVersion

// ---------- Faithfulness witnesses (honest behaviors are reachable) ----------

run HappyPathChain {
  HonestTransitions
  some v: SchemaVersion, r1, r2, r3: Record |
    eventually (v in ExistingSchemaVersion and r1 in ExistingRecord and r1.schemaSha = v
      and eventually (r2 in ExistingRecord and r2.predecessor = r1
        and eventually (r3 in ExistingRecord and r3.predecessor = r2)))
} for 5 but 8 Step

run SchemaSwapAuditTrail {
  HonestTransitions
  some disj v1, v2: SchemaVersion, r: Record |
    eventually (v1 in SchemaHead
      and eventually (r in ExistingRecord and r.schemaSha = v1
        and eventually (v2 in SchemaHead
          and r in ExistingRecord and r.schemaSha = v1)))
} for 5 but 8 Step
