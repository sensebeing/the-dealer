---
id: guard.no-silent-fallback
title: No Silent Fallback
manifest: AGENTS.md#engineering-quality-policy
---

# No Silent Fallback

Never fabricate business facts or hide failures behind defaults, bypasses or recovery paths. Owners may normalize provably equivalent representations, isolate unknown parts, explicitly degrade, recover or hand off under existing contracts. A confirmed failure does not forbid all subsequent processing.

When identity, tenancy, permission, idempotency or actual write safety cannot be established, block only the affected operation. Preserve the reason and distinguish a failed dependency from a valid empty result. Retry only with knowledge of possible prior effects.
