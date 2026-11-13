# Reusable alter-service patterns

These recipes describe responsibility boundaries. Inspect the consuming
project's current adapters and owner API before implementing them.

| Pattern | Source input | Durable identity | Delivery |
| --- | --- | --- | --- |
| Feedback claim | owner claim page + CAS acquisition | entityId and claimVersion | orca (mode: spawn) / orca-spawn task mailbox |
| Quality wave | report date, revision and digest | one cursor per report stream | fixed workspace or host session |
| Alert wave | independent environment/table streams | one committed watermark per stream | coalesced fixed mailbox |
| Collaboration request | collab.request.v4 + team registry | request/thread IDs and revisions | receiver-local inbox and selected mode |

## Feedback claim (source-hub-claim-source style)

The backend owns assignment and claimVersion. Acquire with CAS; a conflict means
another worker owns the claim, not a reason to dispatch anyway. A local `/wake`
notification requests an immediate source pass, with one polling fallback.
Serialize runs and keep a single cursor. Deliver only owner-confirmed claims;
advance the page cursor only after every eligible envelope has a durable receipt.
Do not pre-run the actual diagnosis or export as a second dispatch gate.

## Quality wave (quality-report-wave-source style)

Read reportDate/revision/digest from the report owner. Deduplicate the versioned
identity rather than a timestamp guessed by the source. Enqueue in one fixed
stream, let MailboxStore own leases and WakeupRedriveService own idle wakeup and
rotation. A source watermark is not evidence that the report task completed.

## Alert wave (lark-alert-wave-source style)

Maintain separate watermarks for independent sources/environments. A failed
query is not an empty successful page; retain that stream's cursor while healthy
streams continue. Coalesce delivery through the mailbox rather than adding a
second periodic wake loop. Never inject into a busy or unverifiable terminal.

## Team collaboration (collab.request.v4 style)

The team registry owns member/area routing. The request owner owns transitions;
Dealer owns transport, admission, receipt and mailbox leases. Keep original
request IDs and thread direction for replies and settlement. Use BLAKE3-verified
attachments rather than trusting remote filesystem paths. Test same-machine
bidirectional threads and remote peer routing. A new runtime adapter must not
invent another business state machine or auto-close a request on handoff.
