---
id: guard.test-case-quality
title: Test Case Quality
manifest: AGENTS.md#engineering-quality-policy
---

# Test Case Quality

Resident tests protect current core business facts and declare what their failure means. Assert observable behavior, not tautologies, mock calls alone or mirrored source text. Avoid guessed timing sleeps and unit-test external side effects. One-off presentation, copy and historical examples do not become permanent suites.

Cover core behaviors, related failure families and direct contracts. Reuse evidence when its effective inputs match; distinguish code failure, environment blockage and untested scope. Organize tests around enduring owner contracts, not one tiny symptom file per patch. Use explicit module-level budgets where needed rather than arbitrary per-file line limits; budget revisions require review and cannot silently bypass quality checks.
