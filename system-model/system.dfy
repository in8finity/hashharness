// Dafny 4 model: hashharness server-stamped created_at (I3) and text_sha256
// uniqueness (I1).
//
// Companion to system.als. The Alloy model checks all I1–I5 within bounded
// scope (5 records, 8 time steps). This Dafny model proves I1 and I3
// **unbounded** — for any number of records, any sequence of honest
// CreateItem calls preserves both invariants.
//
// Maps to: src/hashharness/storage.py
//   - create_item              → method CreateItem
//   - server-stamping          → caller-side `now` advanced by the method
//   - rejection of caller-supplied created_at → CreateItem has no `t` parameter
//
// Why unbounded matters: the Alloy proof is conditional on "≤ 5 records, ≤
// 8 time steps" — within scope, no counterexample. The Dafny proof is
// universal — for ANY number of records and ANY interleaving of honest
// CreateItem calls, the invariant holds. The two proofs complement each
// other: Alloy gives counterexamples and SAT witnesses for design-time
// exploration; Dafny gives unbounded guarantees suitable for production.

datatype Record = Record(text_id: nat, created_at: nat)

class HashHarness {
  var records: seq<Record>
  var now: nat

  // Integrity invariant. Three conjuncts:
  //   I1   text_id (≡ text_sha256) is unique across all stored records.
  //   I3a  created_at is strictly monotonic in insertion order.
  //   I3b  every stored record's created_at is strictly less than the
  //        current clock — server has advanced past every stamp.
  ghost predicate Invariant()
    reads this
  {
    && (forall i, j :: 0 <= i < j < |records| ==>
          records[i].text_id != records[j].text_id)
    && (forall i, j :: 0 <= i < j < |records| ==>
          records[i].created_at < records[j].created_at)
    && (forall i :: 0 <= i < |records| ==> records[i].created_at < now)
  }

  constructor()
    ensures Invariant()
    ensures records == []
    ensures now == 0
  {
    records := [];
    now := 0;
  }

  // Honest create_item: server stamps `created_at` from current `now`,
  // appends the record, advances `now`. The caller has no way to influence
  // the timestamp — there is no `t` parameter. Idempotent on text_id
  // collision (returns success=false).
  method CreateItem(text_id: nat) returns (success: bool)
    requires Invariant()
    ensures Invariant()
    ensures success <==>
      !(exists i :: 0 <= i < |old(records)| && old(records)[i].text_id == text_id)
    ensures success ==> |records| == |old(records)| + 1
    ensures success ==> records[..|old(records)|] == old(records)
    ensures success ==> records[|records| - 1] == Record(text_id, old(now))
    ensures success ==> now == old(now) + 1
    ensures !success ==> records == old(records) && now == old(now)
    modifies this
  {
    var i := 0;
    var collision := false;
    while i < |records|
      invariant 0 <= i <= |records|
      invariant collision <==> exists k :: 0 <= k < i && records[k].text_id == text_id
    {
      if records[i].text_id == text_id {
        collision := true;
      }
      i := i + 1;
    }
    if collision {
      success := false;
      return;
    }
    var r := Record(text_id, now);
    records := records + [r];
    now := now + 1;
    success := true;
  }
}

// ----------------------------------------------------------------------
// Adversarial variant: caller supplies the timestamp.
//
// To preserve Invariant() under a caller-supplied timestamp, Dafny requires
// the precondition `t >= h.now` (and we must advance `now` to t+1). Drop
// that precondition and the postcondition `ensures h.Invariant()` cannot be
// proved: nothing forces `t` to exceed prior records' timestamps. This is
// the unbounded analogue of the Alloy `BackdateBypassesIfStampingRelaxed`
// run going SAT — relaxing server-stamping breaks the invariant for any
// store with at least one prior record.
// ----------------------------------------------------------------------

method CreateItemAdversarial(h: HashHarness, text_id: nat, t: nat) returns (success: bool)
  requires h.Invariant()
  // The precondition that has to be added to make the invariant hold —
  // i.e., the caller must supply t = current server clock. Removing this
  // line causes the verifier to reject the method (try it).
  requires t >= h.now
  ensures h.Invariant()
  modifies h
{
  var i := 0;
  var collision := false;
  while i < |h.records|
    invariant 0 <= i <= |h.records|
    invariant collision <==> exists k :: 0 <= k < i && h.records[k].text_id == text_id
  {
    if h.records[i].text_id == text_id {
      collision := true;
    }
    i := i + 1;
  }
  if collision {
    success := false;
    return;
  }
  var r := Record(text_id, t);
  h.records := h.records + [r];
  h.now := t + 1;
  success := true;
}

// ----------------------------------------------------------------------
// Sanity-check lemma: a 3-record honest run preserves the invariant.
// (Dafny needs no help proving this; included as documentation.)
// ----------------------------------------------------------------------

method ThreeRecordHappyPath() returns (h: HashHarness)
  ensures fresh(h)
  ensures h.Invariant()
  ensures |h.records| == 3
  ensures h.records[0].created_at < h.records[1].created_at
                                  < h.records[2].created_at
{
  h := new HashHarness();
  var ok1 := h.CreateItem(100);
  var ok2 := h.CreateItem(200);
  var ok3 := h.CreateItem(300);
  // Dafny derives ok1 == ok2 == ok3 == true from the postconditions
  // (no text_id collision means success).
}
