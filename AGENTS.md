# the-dealer — P2P Agent Dispatcher & Collaboration Mesh

`the-dealer` 是一个去中心化、微内核插件驱动的跨机器、跨团队成员、跨 Agent 实时消息调度与协作网络。`CLAUDE.md` 软链指向本文件；任何 Agent 在本仓库内开发、排查或扩展插件前须遵守以下架构边界与工程质量契约。

---

## 1. 模块唯一职责与非职责边界（Single Owner）

每个核心模块只承担其唯一职责，禁止跨层揉杂或复制第二真源：

| 模块路径 | 唯一语义职责（Owner） | 非职责禁区（Forbidden） |
| :--- | :--- | :--- |
| [`packages/core-types/index.ts`](packages/core-types/index.ts) | 全部跨层接口契约、确定性 `canonicalJson`、`HKDF-SHA256` 网络密钥派生（`deriveNetworkKeys`）、`AES-256-GCM` 帧加解密（`sealNetworkFrame` / `unsealNetworkFrame`）与 Ed25519 签名验签 | 不持有任何运行时状态、I/O 或插件生命周期 |
| [`packages/kernel/index.ts`](packages/kernel/index.ts) | Cordis 风格微内核与生命周期引擎：依赖注入拓扑激活（`inject`）、逆序先停消费者再停提供者、LIFO `unwind()`、五种事件派发语义（`emit / parallel / serial / bail / waterfall`）与 `composeProfile` / `applyPatch` | 不包含任何 P2P 网络、路由、门禁或执行器业务逻辑 |
| [`packages/transport/index.ts`](packages/transport/index.ts) & [`native/dealer-iroh-engine`](native/dealer-iroh-engine/src/lib.rs) | 多网络并发挂载（`mountNetwork`）、基于 `swarmTopicId` 的加密发现信标、带 `requestNonce` 绑定的单播/多播/广播帧收发、流式 `BLAKE3` 校验的大文件分片传输（`shareFile` / `fetchBlobToPath`） | 不解析业务信封语义，不决定路由、门禁或执行 |
| [`packages/node/index.ts`](packages/node/index.ts) | 本机 Ed25519 身份私钥唯一真源（`privateKeyPem`）、按网络隔离的签名 `NodeCard` 周期发布与版本号单调递增校验、多维 Selector 解析（`ed25519:` / `member:` / `skill:` / `member@node` / `network/selector`） | 不直接管理底层 Socket 或执行入站任务 |
| [`packages/router/index.ts`](packages/router/index.ts) & [`packages/plugins-router/index.ts`](packages/plugins-router/index.ts) | 出站消息三级路由判定（Explicit Selector → Capability/Load 规则 → `RouteJudgePlugin` 智能裁判），强制校验裁判返回的 `targetNodeIds` 必须属于当前网络可用候选集 | 不直接发送网络字节流，不把未认证文本拼入 Shell 命令 |
| [`packages/gate/index.ts`](packages/gate/index.ts) | 入站四态门禁（`open` / `approval_required` / `dnd` / `closed`）、按网络覆盖模式、优先级 `GateRulePlugin`、`approve(--trust-sender)` 发送方信任放行与 `drainDnd()` 排空 | 不越过 `dispatcher` 直接调用外部执行器 |
| [`packages/dispatcher/index.ts`](packages/dispatcher/index.ts) & [`packages/plugins-policy/index.ts`](packages/plugins-policy/index.ts) & [`packages/plugins-executor/index.ts`](packages/plugins-executor/index.ts) | 持久化原子账本 `DeliveryLedger`、策略中间件链（`dedup-cas` CAS 水位去重）、门禁放行后统一触发 `dispatcher/inbound` 反向路由事件、以及向 `orca-spawn`（显式指定 `--no-parent` 一级工作区或 `--parent-worktree` 子工作区，执行命令以接收端配置为准，projectId 仅通过接收端 projectRoots / config.projects 解析，绝不信任发送端路径，接收端 parentWorktree 必须验证属于目标仓库）、`orca-wave`、`agent-pty`、`inbox`、`notify` 投递并签发 `DeliveryReceipt` | 不静默吞掉执行错误；未知外部副作用状态（`dispatching` / `failed`）不盲目自动重放 |
| [`packages/mailbox/store.ts`](packages/mailbox/store.ts) | 唯一持久化信箱账本（`~/.dealer/mailbox.json`）：逻辑 `mailboxId` 寻址、单未决投递租约 `dlv_*`、`wakeTargets` 重驱状态、按 `inbound:` / `outbound:` 方向键区分的 `threads` 反向路由（同机回环双向不覆盖、状态向同线程全部记录传播）、旧 `inbox.json` 一次性无损迁移、文件锁 + `fsync`/`rename` 原子落盘、零轮询 `waitForMail` 与 `onMail` / `onWakeTarget` 事件唤醒 | 不解析网络帧、不选择执行器、不决定路由或门禁 |
| [`packages/mailbox/wakeup.ts`](packages/mailbox/wakeup.ts) | `WakeupRedriveService`：持久化 `wakeTargets` 的唯一定时/事件重驱 owner；严格单行指针模板（标准波次、双阶段恢复、spawn 任务前言）、`passive` / `orca-run` / `orca-terminal` 三通道、忙时零注入的权威空闲门禁、1s→2s→5s→10s 指数退避、租约结算后按 `completedWaves` 阈值 `/new` 轮转、Orca 工作区备注更新与 `flushWakeups()` | 不解析业务信封、不直接执行任务、不在未确认空闲时注入终端文本 |
| [`packages/orca-workspace/index.ts`](packages/orca-workspace/index.ts) | Orca CLI 只读检查与受控命令的唯一 owner：递归工作区 `worktree ps --agent-wait` 空闲判定、终端 `tui-idle` 探测、终端解析、单行文本投递与工作区备注更新 | 不决定消息路由、不持有信箱状态、不执行任务语义 |
| [`packages/emitter/index.ts`](packages/emitter/index.ts) & [`packages/plugins-source/index.ts`](packages/plugins-source/index.ts) | 出站消息制备（`emitter/prepare` waterfall）、附件自动 `shareFile`、Ed25519 信封签名、回执签名校验，以及外部事件源（`webhook` / `poll-watermark` / `script`）生命周期托管 | 不允许 `emitter/prepare` 钩子篡改发送方身份或所属 `networkId` |
| [`packages/cli/`](packages/cli/index.ts) & [`bin/`](bin/dealer.js) | 内置 Profile（`personal-dev` / `dedicated-worker` / `headless-relay`）、声明式 JSON/YAML Patch 解析、独占状态目录 `DealerNodeRuntime`（`0700` 目录 + `runtime.lock` + `0600` 私钥与 Unix Control Socket）及 `dealer` / `dealerd` CLI 入口 | 不在 CLI 层重新实现核心服务已有协议或绕过门禁/签名校验 |

---

## 2. 工程质量红线（Product Engineering Quality Rules）

```yaml
schema: product-engineering-quality/v1
rules:
  - id: peq.architecture-simplicity
    severity: block
    summary: 每个职责与安全不变量由唯一 owner 判定；写时/入站边界原子校验，主链直出，拒绝无独立保护作用的冗余包装与表象补丁。

  - id: peq.single-source-of-truth
    severity: block
    summary: 节点签名身份（node.privateKeyPem）、网络派生密钥（deriveNetworkKeys）、路由候选白名单验证（validateDecision）与投递状态账本（DeliveryLedger）各自保持唯一真源，禁止复制第二真源。

  - id: peq.event-driven-execution
    severity: block
    summary: 核心链路通过 Kernel 事件与流式 Socket 帧驱动；轮询采集源（poll-watermark）必须保持单一游标水位且仅在全页投递成功后推进游标；持久化 wakeTargets 必须经权威工作区/终端 idle 校验后才投递单行指针，事件即时触发并以指数退避补齐延迟目标，禁止在忙碌或不可判定时注入文本。

  - id: peq.no-silent-fallback
    severity: block
    summary: 严禁静默吞掉签名校验失败、AEAD 解密失败、BLAKE3 摘要不匹配或子进程非零退出码；未知外部执行结果必须忠实记录为 dispatching/failed，不得伪造成功回执。

  - id: peq.code-organization
    severity: block
    summary: 保持零外部 npm 运行时依赖（仅使用 Node.js >=22.6 内置模块）；禁止函数内联动态 import()、禁止 @ts-ignore/@ts-expect-error、禁止单行多语句分号压缩、禁止遗留死代码。

  - id: peq.security-and-isolation
    severity: block
    summary: 不同 Network 通过 HKDF 派生的 AEAD 密钥与 Header AAD 严格隔离；执行外部命令统一使用 shell: false 参数数组或严格单引号转义；orca-spawn 创建工作区必须显式指定层级策略（独立一级工作区传 --no-parent，子工作区传 --parent-worktree）；projectId 仅通过接收端 projectRoots（config.projects）绑定，忽略发送端 repoPath/cwd/parentWorktree，接收端 parentWorktree 须属于绑定仓库；状态目录与控制 Socket 保持 0700/0600 权限与 runtime.lock 独占锁。

  - id: peq.test-case-quality
    severity: block
    summary: 常驻测试覆盖密码学向量、微内核拓扑与回滚、跨进程 P2P 发现与篡改拒绝、BLAKE3 大文件流式校验、三级路由、四态门禁、CAS 去重、守护进程重启后的持久化唤醒恢复与 `/new` 轮转，以及真实 dealerd 守护进程 E2E 闭环。
```

---

## 3. 开发、验证与文档索引

- **运行环境**：Node.js `>=22.6.0`（原生支持 `--experimental-strip-types` 直接运行 `.ts`，无需编译构建步骤；零外部 npm 依赖）。可选 Rust 原生引擎源码位于 [`native/dealer-iroh-engine/`](native/dealer-iroh-engine/Cargo.toml)。
- **全量测试命令**：
  ```bash
  npm test
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
