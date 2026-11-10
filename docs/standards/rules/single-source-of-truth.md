---
id: guard.single-source-of-truth
title: Single Source of Truth
manifest: AGENTS.md#engineering-quality-policy
---

# Single Source of Truth

Each business rule, state, vocabulary, classification and filter has one executable owner. Services, UI, skills and tests reference that definition or consume its structured output rather than maintaining competing decisions.

Non-owners must not reverse-fetch raw details to recompute authoritative state, summaries or watermarks. When replacing a source or protocol, switch readers, freeze obsolete writes, and delete replaced code, routes and dual-track flags in the same change. No fallback reads, dual writes, mixed discovery across old/new tables, or disclaimers that defer the agreed migration.

Evolve identity in place on the authoritative row and atomically rebind references instead of cloning generations and making consumers reconcile them. If mixed lifecycle entities require defensive filters, fix schema constraints and write semantics; deleting a helper while copying its predicate into callers is not convergence.
