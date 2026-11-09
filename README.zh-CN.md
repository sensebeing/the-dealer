# the-dealer（中文文档）

[English](README.md) | 简体中文

> *"先与同一台机器上的每个 Agent 协作，再跨越任何机器与团队成员。"*
> 把对的任务牌发给对的 Agent：单机多 Agent 协作优先，跨机跨团队 P2P 无缝扩展。

**`the-dealer`** 是一个开源的、基于无中心 P2P 网络与全插件化微内核的 **Agent 协作与派发系统**。它的第一使用形态是**单机多 Agent / 多工作区自动协作与派发中枢**：个人开发者无需任何网络配置，即可在同一台机器上协调多个专用 Agent 工作区（dev/prod 告警波次、反馈与低质会话认领、prod-release 交接、隔离子工作区任务），完成路由、去重、波次聚合、空闲门禁与 Orca 终端投递。

当团队约定并挂载加密协作网络（`networkId + passphrase`）后，同一套微内核、信封、路由、门禁与执行器无需修改即可渐进扩展为**跨机器设备互联、跨团队成员 P2P 协作网络**，并且依然不需要中心服务器。设计灵感汲取自 **DeepSeek Harness (`Cordis`)** 的 *"Everything is a Plugin"* 架构范式，底层结合 **Iroh (QUIC + Gossip + Blobs)** 高性能点对点网络设计。

---

## 两步式协作：Local First，P2P 渐进扩展

1. **第一步 — 单机多 Agent 协作（本地优先，零网络配置）**
   运行 `dealer node up` 后直接向本机节点发送消息（`member:<self>` 或 `skill:<local-skill>`）。无需 `network mount`、`passphrase`、节点发现或任何外部网络；消息仍完整经过 `gate` → `dispatcher` → `policy` → `executor` 链路，因此单机任务同样享有幂等去重、波次聚合、空闲门禁与 Orca 工作区自动派发。项目清单中的 `defaultNetworkId` 只是其触发源在本机发送时使用的稳定作用域名称。

2. **第二步 — 跨机器 / 跨团队成员 P2P 扩展（`networkId + passphrase`）**
   团队成员约定 `networkId` 与共享口令 `passphrase`，各自执行 `dealer network mount`。本地通过 `HKDF-SHA256 / BLAKE3` 确定性派生出加密发现频道 `swarmTopicId` 与 AEAD 帧密钥 `sealedKey`；签名 `NodeCard`、多维选择器（`member:` / `skill:` / `member@node` / `network/selector`）、四态入站门禁与全部执行器在加密网络内继续原样工作。

---

## 为什么选择 `the-dealer`？

现代 AI 工程团队会在一台开发机上运行多个异构 Agent（**Antigravity**、**Claude Code**、**Codex**、**DeepSeek Harness**）与相互隔离的工作区（如 **Orca** worktree），随后才需要跨机器、跨成员协作。现有方案通常存在三类瓶颈：

1. **依赖 Git 的协作请求慢且依赖人工**：把 Markdown 请求提交进仓库、等待 Webhook / 飞书卡片、再手动复制 Prompt 到新工作区，延迟高且充满重复劳动。
2. **彼此孤立的本地派发脚本**：用户反馈、服务告警、iOS 崩溃符号化、低质会话审查、prod 发布波次等场景通常各自维护一套只耦合单机的轮询守护进程，没有共享的路由、门禁与幂等模型。
3. **中心化 HTTP 协议（如 A2A）缺少 P2P 发现与本地工作区驱动能力**：标准 Web 协议假设公网 HTTPS 端点，缺少零配置 NAT 穿透、加密网络隔离、节点级准入状态机（`open` / `approval_required` / `dnd` / `closed`）与本地工作区启动器。

`the-dealer` 先以零配置本地回环解决单机多 Agent 协作，再用同一套契约承载跨机加密协作，而不是维护第二套派发栈。

---

## 核心特性

1. **单机回环与加密多网络挂载（Multi-Network Mounting，`networkId + passphrase`）**
   - 单机模式：未挂载网络时，发往本机节点的帧直接在进程内经同一入站处理器投递；已挂载网络时，本机目标复用本地 endpoint。
   - 团队即网络（`Network`）：团队成员约定 `networkId`（如 `sensebeing`）与共享口令 `passphrase`，本地确定性派生出发现频道指纹与帧加密准入密钥。
   - 单节点可同时挂载多个网络（如个人私域网 `ben-home` + 团队协作网 `sensebeing`），并为每个网络独立配置入站权限门禁与暴露的技能集。
2. **高性能 P2P 传输与大附件流（`transport`）**
   - 支持**定向单播（Unicast）**、**批量多播（Multicast）**、**千节点级频道广播（Broadcast）**。
   - 原生支持内容寻址的 **多媒体/大文件附件传输（`BlobAttachment`）**：截图、音视频、百兆 DebugLog 诊断包、Git Patch 按需流式拉取至本地 `0700` 隔离目录。
3. **节点身份与能力名片（`node` & `NodeCard`）**
   - 每个节点拥有 Ed25519 公钥身份（`nodeId`）、成员标识（`member`）、节点名（`nodeName`）以及类 Agent Skill 的自然语言+结构化标签能力描述（`skills`），随 Gossip 心跳实时同步负载状态（`idle | busy | overloaded`）。
   - 选择器解析优先使用已发现的远端节点，找不到时回退到本机自身 `NodeCard`，因此单机路由无需任何远端 Peer。
4. **高自由度发送器与 AI 发送判断器（`emitter` & `router`）**
   - 外部触发源插件化：支持 Webhook、单水位轮询（Lark 告警 / 反馈认领 / 低质会话认领）、自定义脚本或节点内 Agent 直接调用。
   - 三级智能路由：显式身份直指（`member:oliver`）→ 技能标签匹配（`skill:prod-release`）→ **AI 发送判断器插件**（挂载本地 AGY / Claude Code / Codex 或 LLM API 读取在线节点 `NodeCard` 自动裁决单播/多播目标）。本机自身名片始终是合法候选。
5. **节点自主权限门禁（`gate`）**
   - 四态权限控制：`open`（直接放行）、`approval_required`（Knock 敲门待确认模式，确认后可一键签发发送方信任白名单）、`dnd`（免打扰仅入队）、`closed`（拒收）。
   - 本机自身发起的本地投递跳过敲门审批，但仍严格受 `dnd` 与 `closed` 约束。
6. **插件化接收派发器（`dispatcher`）**
   - **调度策略插件**：内置 `policy-dedup-cas`（代次水位 CAS 防重）与 `policy-wave-coalescer`（单水位波次聚合 + 按每条流的 `workspacePath`/`terminalHandle` 动态解析工作区空闲门禁 + 会话满阈值自动 `/new` 轮转）。
   - **终端执行器插件**：内置 `executor-orca-spawn`（自动新建 Orca 独立工作区并启动 Agent）、`executor-orca-wave`（按流解析目标工作区与活跃 antigravity 终端、投递波次并更新卡片状态）、`executor-agent-pty`（无 Orca 环境的通用 CLI Agent 执行器）与 `executor-notify`。

---

## 详细设计文档导航

- [整体架构与六大核心模块契约](docs/architecture.md)
- [P2P 帧协议、多网络密钥派生与大文件流规范](docs/wire-protocol.md)
- [插件开发指南（Source / Router / Gate / Policy / Executor）](docs/plugin-guide.md)
- [配套 Agent Skill (`skills/the-dealer/SKILL.md`)](skills/the-dealer/SKILL.md)

## 快速开始

### 第一步 — 单机多 Agent 协作（零网络配置）

```bash
# 运行完整测试套件（Node.js >= 22）
npm test

# 启动本机节点并查看状态
./bin/dealer.js node up --profile personal-dev --member ben.xu --name mbp --skills agent-runtime --json
./bin/dealer.js node status --json

# 向本机投递；`local` 只是稳定作用域名，无需 mount 或 passphrase
./bin/dealer.js send --network local --to member:ben.xu --topic collab.request.v1 --body '{"summary":"执行本地质量巡检"}' --json
./bin/dealer.js send --network local --to skill:agent-runtime --topic triage --body '{"area":"agent-runtime"}' --json
```

已 link 的项目清单中的 `defaultNetworkId` 就是其触发源在本机发送时使用的稳定作用域；单机消息仍会路由到本机自身名片。

### 第二步 — 跨机器与跨团队成员 P2P 扩展

```bash
# 每台团队机器挂载同一个加密协作网络
./bin/dealer.js network mount sensebeing --passphrase "team-shared-secret" --member ben.xu --node mbp-main --json
./bin/dealer.js peers list --network sensebeing --json

# 在加密网络内向其他机器发送任务
./bin/dealer.js send --network sensebeing --to member:oliver --topic collab.request.v1 --body '{"summary":"Review the release"}' --json
```
