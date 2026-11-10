# Request threads and local topic delivery

A sender can group ordinary messages into an existing thread without changing the
message kind:

```sh
dealer send --network team --to member:alice --topic request --thread discussion-1 --body 'Please review'
dealer thread list --topic request --state open --limit 20 --json
dealer thread show discussion-1 --json
```

`broadcast` accepts the same `--thread` flag. Thread history preserves each
signed envelope, including earlier hops after a reply or settlement updates the
thread state. Legacy thread records without envelopes remain readable.

## Receiver-local bindings

Configure `topicBindings` in the node configuration or a linked project's user
patch. A project patch takes precedence over node configuration; conflicting
bindings from multiple linked projects are rejected.

```json
{
  "topicBindings": {
    "request": {
      "executor": "orca-wave",
      "streamKey": "requests",
      "projectId": "example",
      "workspacePath": "/workspaces/example/requests",
      "autoCreateWorkspace": true,
      "workspaceComment": "Incoming requests",
      "wakeupPrefix": "Read the project guide before handling this batch.",
      "coalesceMs": 2000,
      "rotateAfterWaves": 6,
      "notifyLark": true
    }
  }
}
```

The executor must be enabled in the node profile. A simple inbox binding is
`{"executor":"inbox"}`; `mailboxId` can override the bound mailbox. Wave bindings
otherwise use `stream:<streamKey>` (or `stream:<kind>`). Local `projectId` resolves
through linked projects; a project user patch can also use its own linked root.
Bindings do not rewrite signed inbound messages.

Ordinary messages with an explicit executor keep that executor. Session control
messages (`question`, `reply`, `settle`) always use inbox delivery; when their
semantic target is absent or `default`, they follow the local topic binding.
Explicit non-default targets remain explicit. Outbound messages without an
explicit reply mailbox advertise the sender's local bound mailbox before final
signing.

Wave wake targets are registered before the first batch. Auto-creation requires
a receiver-local repository root and creates a fixed top-level Orca workspace.
A newly spawned terminal must report `tui-idle` before receiving any pointer or
rotation command. Orphaned terminals cannot satisfy terminal resolution or
poison an otherwise observable idle inventory.

## Optional Lark notification

```json
{
  "larkNotify": {
    "enabled": true,
    "command": "lark-cli",
    "profile": "work",
    "target": { "chatId": "chat_identifier" },
    "includeKinds": ["request"]
  }
}
```

Choose either `target.chatId` or `target.userId`. Without a target, the notifier
uses `lark-cli auth status` to discover the authenticated user's open ID and
prefers an available bot identity. `as` can explicitly select `bot` or `user`.
The CLI is invoked as an argument array without a shell.

By default, notification covers `default` plus bound mailboxes whose binding does
not set `notifyLark: false`. Explicit `mailboxes` replaces that default set (an
empty array disables all mailbox notifications). `excludeMailboxes`,
`includeKinds`, `excludeKinds`, and `skipSelfMessages` provide further filtering.
Linking or unlinking projects refreshes the default set for subsequent arrivals.

Payloads may provide `notification.markdown` or member-specific
`notification.recipientMarkdown`. Otherwise notifications contain a generic
subject, sender, mailbox, priority, thread ID, body, and attachment names.
Follow-ups reply in the first Lark message's thread. A deleted root permits one
new-root send with a distinct deterministic idempotency key.

The notifier observes mailbox events and performs a startup catch-up. A failed
item fences later sequence numbers until a bounded retry succeeds; filtered
items still advance the cursor. State is stored in `lark-notify.json` with mode
0600 and atomic replacement. Mailbox sequence numbers start at zero, so a fresh
notifier uses cursor -1. Restart retries reuse the same idempotency key. Stopping
the runtime cancels notifier retries and aborts pending commands.
