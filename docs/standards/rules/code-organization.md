---
id: guard.code-organization
title: Code Organization
manifest: AGENTS.md#engineering-quality-policy
---

# Code Organization

Modules, files, classes and methods have clear cohesive responsibilities and names. Delete replaced code; avoid duplicate entry points, unrelated responsibilities, deep nesting and ownerless utility dumping grounds.

Judge cohesion, explicit input/output contracts, single ownership and acyclic dependency direction rather than arbitrary file/function line limits. Extract independent domain responsibilities through explicit parameters or composition. Do not mechanically split numbered fragments, inject globals across modules, or share implicit mutable state through mixin chains and same-name MRO shadowing.

Architecture subtraction must remove runtime branches, dual-track flags, duplicate writes and defensive predicates. Moving two state machines into one file, renaming symbols or inlining duplicated expressions is cosmetic subtraction. Declare imports at module top level; resolve cycles through contract placement rather than function-local imports or E402 suppression. Do not compress multiple statements with semicolons.
