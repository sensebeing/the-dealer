---
id: guard.service-boundaries
title: Service Boundaries
manifest: AGENTS.md#engineering-quality-policy
---

# Service Boundaries

Modules perform only the responsibilities and decisions declared in their owner documentation. Shared libraries, UI and infrastructure provide generic mechanisms or expose owner-defined structured semantics; they do not duplicate vertical business rules.

Cross-module changes identify affected read/write entry points, the sole semantic owner, direct consumers, dependency direction and compatible delivery order. Review the end-to-end call topology, including asynchronous updates, withdrawals, merges, migrations and filters.
