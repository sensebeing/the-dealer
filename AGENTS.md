# the-dealer — P2P Agent Dispatcher & Collaboration Mesh

`the-dealer` 是一个去中心化、微内核插件驱动的跨机器、跨团队成员、跨 Agent 实时消息调度与协作网络。`CLAUDE.md` 软链指向本文件；任何 Agent 在本仓库内开发、排查或扩展插件前须遵守以下架构边界与工程质量契约。

---

## 1. 模块唯一职责与非职责边界（Single Owner）

每个核心模块只承担其唯一职责，禁止跨层揉杂或复制第二真源：

| 模块路径 | 唯一语义职责（Owner） | 非职责禁区（Forbidden） |
| :--- | :--- | :--- |
| [`packages/core-types/index.ts`](packages/core-types/index.ts) | 全部跨层接口契约、确定性 `canonicalJson`、`HKDF-SHA256` 网络密钥派生（`deriveNetworkKeys`）、`AES-256-GCM` 帧加解密（`sealNetworkFrame` / `unsealNetworkFrame`）与 Ed25519 签名验签 | 不持有任何运行时状态、I/O 或插件生命周期 |
| [`packages/process-lock/index.ts`](packages/process-lock/index.ts) | 崩溃可恢复 owner 文件（`runtime.lock` / `mailbox.json.lock`）的唯一记录格式与存活判定：进程实例 token、容器身份，以及本进程当前持有的目录集（按 realpath 归一） | 不获取或释放具体锁，不决定锁粒度、重试与回收流程 |
| [`packages/kernel/index.ts`](packages/kernel/index.ts) | Cordis 风格微内核与生命周期引擎：依赖注入拓扑激活（`inject`）、逆序先停消费者再停提供者、LIFO `unwind()`、五种事件派发语义（`emit / parallel / serial / bail / waterfall`）与 `composeProfile` / `applyPatch` | 不包含任何 P2P 网络、路由、门禁或执行器业务逻辑 |
| [`packages/transport/index.ts`](packages/transport/index.ts) & [`native/dealer-iroh-engine`](native/dealer-iroh-engine/src/lib.rs) | 多网络并发挂载（`mountNetwork`）、基于 `swarmTopicId` 的加密发现信标、带 `requestNonce` 绑定的单播/多播/广播帧收发、流式 `BLAKE3` 校验的大文件分片传输（`shareFile` / `fetchBlobToPath`） | 不解析业务信封语义，不决定路由、门禁或执行 |
| [`packages/node/index.ts`](packages/node/index.ts) | 本机 Ed25519 身份私钥唯一真源（`privateKeyPem`）、按网络隔离的签名 `NodeCard` 周期发布与版本号单调递增校验、多维 Selector 解析（`ed25519:` / `member:` / `skill:` / `member@node` / `network/selector`） | 不直接管理底层 Socket 或执行入站任务 |
| [`packages/router/index.ts`](packages/router/index.ts) & [`packages/plugins-router/index.ts`](packages/plugins-router/index.ts) | 出站消息三级路由判定（Explicit Selector → Capability/Load 规则 → `RouteJudgePlugin` 智能裁判），强制校验裁判返回的 `targetNodeIds` 必须属于当前网络可用候选集 | 不直接发送网络字节流，不把未认证文本拼入 Shell 命令 |
| [`packages/gate/index.ts`](packages/gate/index.ts) | 入站四态门禁（`open` / `approval_required` / `dnd` / `closed`）、按网络覆盖模式、优先级 `GateRulePlugin`、`approve(--trust-sender)` 发送方信任放行与 `drainDnd()` 排空 | 不越过 `dispatcher` 直接调用外部执行器 |
| [`packages/dispatcher/index.ts`](packages/dispatcher/index.ts) & [`packages/plugins-policy/index.ts`](packages/plugins-policy/index.ts) & [`packages/plugins-executor/index.ts`](packages/plugins-executor/index.ts) | 持久化原子账本 `DeliveryLedger`、策略中间件链（`dedup-cas` CAS 水位去重）、门禁放行后统一触发 `dispatcher/inbound` 反向路由事件、`activeCount` / `onIdle` / `waitForIdle` idle 门控供受控 drain 与重启使用、以及向 `orca-spawn`（显式指定 `--no-parent` 一级工作区或 `--parent-worktree` 子工作区，执行命令以接收端配置为准，projectId 仅通过接收端 projectRoots / config.projects 解析，绝不信任发送端路径，接收端 parentWorktree 必须验证属于目标仓库）、`orca-wave`、`agent-pty`、`inbox`、`notify` 投递并签发 `DeliveryReceipt` | 不静默吞掉执行错误；未知外部副作用状态（`dispatching` / `failed`）不盲目自动重放 |
| [`packages/agent-host/index.ts`](packages/agent-host/index.ts) | 跨模块共用的宿主 CLI 调用参数构造器（`buildAgentHostInvocation`，支持 `codex` / `claude-code` / `antigravity` 的 `spawn` 与 `fixed` 会话） | 不持有状态、不直接启动子进程、不反向依赖 `mailbox` 或 `plugins-executor` |
| [`packages/mailbox/store.ts`](packages/mailbox/store.ts) | 唯一持久化信箱账本（`~/.dealer/mailbox.json`）：逻辑 `mailboxId` 寻址、单未决投递租约 `dlv_*`、`wakeTargets` 重驱状态、按 `inbound:` / `outbound:` 方向键区分的 `threads` 反向路由（同机回环双向不覆盖、状态向同线程全部记录传播）、旧 `inbox.json` 一次性无损迁移、文件锁 + `fsync`/`rename` 原子落盘、零轮询 `waitForMail` 与 `onMail` / `onWakeTarget` 事件唤醒 | 不解析网络帧、不选择执行器、不决定路由或门禁 |
| [`packages/mailbox/wakeup.ts`](packages/mailbox/wakeup.ts) | `WakeupRedriveService`：持久化 `wakeTargets` 的唯一定时/事件重驱 owner；严格单行指针模板（含最新消息内联预览、双阶段恢复、spawn 任务前言）、`passive` / `orca-run` / `orca-terminal` 三通道、忙时零注入的权威空闲门禁（识别 Turn Autopilot 预填并在根工作区投递前发送 `Ctrl+U` 清行）、1s→2s→5s→10s 指数退避、租约结算后按 `completedWaves` 阈值 `/new` 轮转、Orca 工作区备注更新与 `flushWakeups()` | 不解析业务信封、不直接执行任务、不在未确认空闲时注入终端文本 |
| [`packages/orca-workspace/index.ts`](packages/orca-workspace/index.ts) | Orca CLI 只读检查与受控命令的唯一 owner：递归工作区 `worktree ps --agent-wait` 空闲判定、终端 `tui-idle` 探测、终端解析、单行文本投递与工作区备注更新 | 不决定消息路由、不持有信箱状态、不执行任务语义 |
| [`packages/emitter/index.ts`](packages/emitter/index.ts) & [`packages/plugins-source/index.ts`](packages/plugins-source/index.ts) | 出站消息制备（`emitter/prepare` waterfall）、附件自动 `shareFile`、Ed25519 信封签名、回执签名校验，以及外部事件源（`webhook` / `poll-watermark` / `script`）生命周期托管 | 不允许 `emitter/prepare` 钩子篡改发送方身份或所属 `networkId` |
| [`packages/cli/`](packages/cli/index.ts) & [`bin/`](bin/dealer.js) | 内置 Profile（`personal-dev` / `dedicated-worker` / `headless-relay` 及宿主预设 `orca-dev` / `codex-dev` / `claude-dev` / `antigravity-dev`）、声明式 JSON/YAML Patch 解析、独占状态目录 `DealerNodeRuntime`（`0700` 目录 + `runtime.lock` + `0600` 私钥与 Unix Control Socket）及 `dealer` / `dealerd` CLI 入口 | 不在 CLI 层重新实现核心服务已有协议或绕过门禁/签名校验 |

---

## 2. Engineering Quality Policy

本仓库接入 `the-guard` 架构拓扑检查与合入前独立 Agent 审查门禁（`guard check` / `guard review`）。本仓库使用 GitHub PR 模式（`review.integration_mode=github-pr`）：变更须提交在功能分支上并推送到 `origin`，用 `gh pr create` 创建 GitHub PR，以 `guard review --repo-root . --github-pr sensebeing/the-dealer#<number>` 发布独立审查（由 `the-guard-reviewer[bot]` 署名），再用 `gh pr merge` 合入；不得直接推送 `main`，本地 `pre-push` 钩子会拒绝此类推送。除下方 `the-guard-policy/v1` 规则外，密码学与命令执行边界须始终满足：
- 不同 Network 通过 HKDF 派生的 AEAD 密钥与 Header AAD 严格隔离；
- 执行外部命令统一使用 `shell: false` 参数数组或严格单引号转义，绝不拼接未认证文本；
- `orca-spawn` 创建工作区必须显式指定层级策略（`--no-parent` 或 `--parent-worktree`），`projectId` 仅通过接收端 `projectRoots`（`config.projects`）解析，忽略发送端路径；
- 状态目录与控制 Socket 保持 `0700` / `0600` 权限与 `runtime.lock` 独占锁。

<!-- the-guard-policy:start -->
```yaml
schema: the-guard-policy/v1
rules:
  - id: guard.architecture-simplicity
    severity: block
    scope: [backend, frontend, api, schema, shared, tool, skill, prompt, test]
    summary: Every responsibility, business state, and write decision has one explicit owner.
    details: docs/standards/rules/architecture-simplicity.md
    checker: static-plus-review
  - id: guard.single-source-of-truth
    severity: block
    scope: [backend, frontend, api, schema, shared, tool, skill, prompt, test]
    summary: Each business rule, state, vocabulary, classification and filter has one executable owner.
    details: docs/standards/rules/single-source-of-truth.md
    checker: static-plus-review
  - id: guard.service-boundaries
    severity: block
    scope: [backend, frontend, api, schema, shared, tool, skill, prompt, test]
    summary: Modules perform only the responsibilities and decisions declared in their owner documentation.
    details: docs/standards/rules/service-boundaries.md
    checker: static-plus-review
  - id: guard.code-organization
    severity: block
    scope: [backend, frontend, api, schema, shared, tool, skill, prompt, test]
    summary: Modules, files, classes and methods have clear cohesive responsibilities and names.
    details: docs/standards/rules/code-organization.md
    checker: static-plus-review
  - id: guard.event-driven-execution
    severity: block
    scope: [backend, tool, skill, prompt]
    summary: Real events or messages from the authoritative owner drive execution, watermark queues, and idle-gated wakeups.
    details: docs/standards/rules/event-driven-execution.md
    checker: static-plus-review
  - id: guard.no-silent-fallback
    severity: block
    scope: [backend, api, schema, skill, prompt, tool]
    summary: Never fabricate facts or mask cryptographic, transport, or subprocess failures behind silent fallbacks.
    details: docs/standards/rules/no-silent-fallback.md
    checker: static-plus-review
  - id: guard.process-adjunct-not-truth
    severity: block
    scope: [backend, api, schema, tool, skill, prompt, test]
    summary: Process metadata is not business truth; recovery and continuation rely on durable mailbox obligations and safe retryability.
    details: docs/standards/rules/process-adjunct-not-truth.md
    checker: static-plus-review
  - id: guard.agent-skill-prompt
    severity: block
    scope: [skill, prompt, tool]
    summary: Skills, prompts, CLI contracts, and runtime gates stay symmetric without duplicating state machines.
    details: docs/standards/rules/agent-skill-prompt.md
    checker: static-plus-review
  - id: guard.test-case-quality
    severity: block
    scope: [test, backend, tool]
    summary: Resident tests protect core invariants and observable behavior across crypto, P2P, mailbox recovery, and daemon round trips.
    details: docs/standards/rules/test-case-quality.md
    checker: static-plus-review
```
<!-- the-guard-policy:end -->

---

## 3. 开发、验证与文档索引

`dealer node down` drains and stops the daemon, while `dealer node restart` performs the same drain before starting a fresh process; a drain that cannot quiesce leaves the daemon serving and reports `stopped:false`, with `--force` as the explicit override. `dealer self-update` updates a clean main checkout and repairs skill links. The daemon records `runtimeFingerprint`; its own watcher is the sole automatic reload owner, triggering an idle-gated drain and restart that preserves in-flight dispatches.

- **运行环境**：Node.js `>=22.15.0`（原生支持 `--experimental-strip-types` 直接运行 `.ts`，无需编译构建步骤；零外部 npm 依赖）。可选 Rust 原生引擎源码位于 [`native/dealer-iroh-engine/`](native/dealer-iroh-engine/Cargo.toml)。
- **测试与架构门禁命令**：
  ```bash
  npm test
  guard check --base origin/main
  guard review
  ```
- **CLI 本地运行**：
  ```bash
  ./bin/dealer.js node status --json
  ```
- **核心设计文档与扩展指南**：
  - 架构设计真源：[`docs/architecture.md`](docs/architecture.md)
  - 线协议与加密帧规范：[`docs/wire-protocol.md`](docs/wire-protocol.md)
  - 插件开发指南：[`docs/plugin-guide.md`](docs/plugin-guide.md)
  - Agent 操作 Skill：[`skills/the-dealer/SKILL.md`](skills/the-dealer/SKILL.md)

### 接入、扩展与日常操作三件套

- 接入向导：[`the-dealer-setup`](skills/the-dealer-setup/SKILL.md)，按环境探测、网络选择、同机编排、跨机提示词、同事宿主绑定五步配置。
- 扩展指南：[`the-dealer-extend`](skills/the-dealer-extend/SKILL.md)，个人 Patch、项目插件与团队 Topic/路由/门禁。
- 日常操作：[`the-dealer`](skills/the-dealer/SKILL.md)，发送、回复、收件箱租约和结算。
- 只读接入命令：`dealer setup probe --root <repo> --json`、`dealer setup invite-prompt --network <id> --json`、`dealer setup binding-preset --host <host> --mode <spawn|fixed|inbox-only> --json`。预设生成不会自动写配置或开启执行；邀请包含共享密钥，不得提交入库。
