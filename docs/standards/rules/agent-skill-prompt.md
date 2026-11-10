---
id: guard.agent-skill-prompt
title: Agent, Skill and Prompt Contracts
manifest: AGENTS.md#engineering-quality-policy
---

# Agent, Skill and Prompt Contracts

Skills, prompts and agent configuration reference authoritative services and structured results; they do not copy state machines, business vocabularies or classification logic. Multi-step orchestration belongs in skills; business decisions and authorization belong to owner services. Expose only semantics the model needs.

Runtime deterministically injects required prior context or an explicit match/missing result. Align system prompts, session instructions, skills and tool schemas; remove contradictory defaults together. Code enforces tenancy, irreversible side effects, confirmation state and prerequisite gates. Models interpret user intent within those boundaries; do not add redundant candidate allowlists when the authoritative user-turn authorization and owner write protections already cover the risk.

A skill must not duplicate tool parameter/enum dictionaries or receipt schemas, whether handwritten or generated. The live tool schema and structured response are their single sources. Skills describe orchestration, decision criteria and tradeoffs, with valid links.
