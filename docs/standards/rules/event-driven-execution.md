---
id: guard.event-driven-execution
title: Event-driven Execution
manifest: AGENTS.md#engineering-quality-policy
---

# Event-driven Execution

Real events or messages drive execution and are durably accepted by one owner. Do not introduce or expand polling or timers without explicit approval of the scope; previously approved unchanged scope remains valid.

Queue discovery, claims and backlog observation share one watermark predicate, such as needed > processed. Every claimed row settles on every success, return and exception path: advance the completed watermark, record retry/backoff, or park terminal failures. Never return from a claim without progress.

Exclude retries until their backoff expires and prioritize new work so failures cannot monopolize slots. Events arriving during execution atomically raise the target watermark; settlement advances only the snapshot actually completed, leaving newer work naturally pending without loss or concurrent reentry.
