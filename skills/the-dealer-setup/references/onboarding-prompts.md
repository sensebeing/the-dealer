# Copyable onboarding prompts

Generate fresh invitations instead of keeping secrets in reusable templates:

```sh
dealer setup invite-prompt --network team --member alice --peer-member bob --name laptop --seed reachable.example:4000 --ttl 1h --project demo --json
```

The JSON contains `networkId`, `inviteUri`, `crossMachinePrompt`, and
`teammatePrompt`. The first prompt retains the member identity across machines;
the second assigns the specified peerMember and asks for its own conversation
mode. Both use the network invite encoder's expiry and seed format, preserve
existing node identity, and include peers/send verification. Provide a concrete
peer selector when duplicate member names exist and check both directions.

With an existing network, its configured secret and seeds are reused unless
explicitly overridden. Without one, a fresh secret is generated in the invite;
no node, key or network is created. Mount the returned invite on the originating
machine as well. Invite generation is read-only both offline and with dealerd
running. `--passphrase` overrides only the generated invite, not mounted state.

Share the prompt privately with the intended recipient. Do not commit it to a
project. Replies and final settlement must be observed separately from executor
handoff. Keep requests and approvals within each recipient's configured gate.
