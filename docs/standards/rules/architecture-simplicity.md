---
id: guard.architecture-simplicity
title: Architecture Simplicity
manifest: AGENTS.md#engineering-quality-policy
---

# Architecture Simplicity

Every responsibility, business state, and write decision has one explicit owner. Decide each fact once; downstream consumers use structured owner results. Converge changes, withdrawals, generations, and projections atomically in write or event paths. Reads consume authoritative snapshots without write locks, lazy migrations, model calls, or reverse-fetching data to recompute state.

Derive the minimal target architecture from the business goal and the applicable architecture, module, and task blueprints. Identify authoritative entity tables, ownership of lifecycle transitions, atomic writes, and direct read projections. Compare that design with the whole touched subsystem in the target Git snapshot, including schema, migrations, writers, and readers outside the changed lines.

Extend the authoritative entity lifecycle or projection in place. Block parallel state machines, shadow process tables, redundant outboxes, and adapter branches that duplicate that owner's responsibilities. For consolidation or root-cause repair, block surviving dual persistence, split lifecycles, cross-table triggers that write business state, and wrapper or formatting changes that leave the redundant behavior live. Keep findings tied to the change and its intended scope.

Immutable Anchor-0 blueprints, referenced audit evidence, and baseline complexity budgets from the merge base together with retirement commitments, convergence requirements, and target complexity budgets established in prior reviewed branch blueprints define binding acceptance scope. Block under `guard.architecture-simplicity` or `guard.single-source-of-truth` when editing or deleting a merge-base contract or prior reviewed branch blueprint, or removing a reference, serves to abandon or defer an Anchor-0 root-cause repair, drop a named table, trigger, or state-machine retirement, defer committed convergence to a later wave, or relax a merge-base or reviewed target complexity budget. Allow re-scoping only when the developer note cites explicit user authorization and schema and lifecycle evidence establishes that the surviving entity is genuinely orthogonal. Allow removal of cross-module intrusion or overdesign introduced in a prior reviewed branch blueprint when the change converges on the simplest implementation at the sole owner and preserves all Anchor-0 obligations, committed retirements, convergence requirements, target budgets, and equivalent safety boundaries.

Block column-inlining mirages when a consolidation removes a child process or outbox table by copying its prefixed columns and secondary `*_status` or `*_state` lifecycle onto a parent table while the split state machine, claim loop, or dual track remains live.

Prefer merging and deleting mechanisms over adding defensive rechecks. Every defense must identify its invariant, responsible boundary, and necessary failure scope. Remove duplicate decisions within the same trust and validity scope, speculative compatibility branches, and process proofs that unnecessarily block ordinary business. Isolate only operations whose safety is unknown; do not freeze unrelated service or release flows.

Preserve input validation, authentication, authorization, tenancy, ownership, idempotency, concurrency and integrity checks at trust boundaries and actual writes. Earlier checks or type declarations do not cover intervening changes. A transactional constraint, compare-and-swap witness, or valid authorization at write time provides independent protection.

When redundant model decisions, state machines, dual tracks or procedural gates are discovered, identify the real invariant and propose the simplest owner/code boundary that protects it. Historical commits, reversions and contracts do not exempt overdesign. Obtain explicit authorization before changing a safety gate or contract; do not ask again when already authorized. Reviewers must accept authorized simplification with demonstrated equivalent protection, while blocking missing protection or further symptom patches.
