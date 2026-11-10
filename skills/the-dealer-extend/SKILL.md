---
name: the-dealer-extend
description: Extend Dealer with personal overrides, project source plugins, team topics and collaboration routes, or custom Agent and Bot executors.
---

# Extend Dealer

Use [the-dealer-setup](../the-dealer-setup/SKILL.md) for installation and host
selection; use [the-dealer](../the-dealer/SKILL.md) for running existing workflows.
Read the project's AGENTS.md, `docs/plugin-guide.md`, and the current owner
interfaces before modifying automation. Establish the business owner and the
observable receipt first; choose the smallest applicable extension level.

## Personal

Put receiver-specific bindings in `~/.dealer/projects/<id>.patch.json` and
personal plugins in `~/.dealer/plugins/*.ts`. Generate host bindings using
`dealer setup binding-preset`; merge only intended topic keys. Keep local paths,
profiles, Bot IDs and notification targets out of shared project configuration.
Use `dealer project link <repo> --user-patch <file> --json` to apply the existing
project loader lane. Check `dealer project status <id> --json` after loading.

## Project

Declare shared topics and plugin references in `.dealer/project.json`; implement
source adapters in `.dealer/plugins/*.ts`. Use the external plugin factory API
and zero external npm runtime dependencies. Do not inline dynamic imports.
Sources translate owner events into envelopes; the dispatcher owns gates,
execution and receipts. Store CAS/revision state only at its designated owner.

Start with the [alter-service patterns](references/alter-service-patterns.md):
feedback claims, quality waves, alert waves or collaboration requests. These are
reusable architecture recipes, not claims that local example files were copied
or deployed. Validate current owner APIs before creating a concrete adapter.

## Team and new hosts

Define versioned topics (for example `collab.request.v4`), member/capability
routes, admission policy and typed payloads. Map receiver project IDs locally;
never execute sender paths or interpolate incoming text into shell commands.
A RouteJudge chooses only from authenticated current network candidates. A gate
rule admits work; it never executes it. An ExecutorPlugin invokes the external
host using shell:false argv, preserves nonzero exits and unknown side effects,
and returns a handoff receipt distinct from business completion.

## Verify and deliver

Add resident tests at the true boundary: CAS conflicts, watermark advancement
only after successful delivery of the whole page, busy and unknown idle states,
restart recovery, failed host commands and verified attachment digests. Include
a real dealerd round trip where runtime integration changed. Run `npm test` and
`git diff --check`. Report mocked contracts, live host checks, deployment and
business outcomes separately. Deliver through the repository's review lane.
