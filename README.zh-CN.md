# the-dealer（中文文档）

[English](README.md) | 简体中文

> *"Deal the right task to the right agent, across any machine."*
> 把对的任务牌发给对的 Agent，跨越任何机器与团队成员。

**`the-dealer`** 是一个开源的、基于无中心 P2P 网络与全插件化微内核的**跨 Agent / 跨机器 / 跨团队成员实时事件驱动协作与派发系统**。

设计灵感汲取自 **DeepSeek Harness (`Cordis`)** 的 *"Everything is a Plugin"* 架构范式，底层结合 **Iroh (QUIC + Gossip + Blobs)** 高性能点对点网络设计，让每一台开发机或 Agent 工作区都能通过一行命令挂载到加密的**协作网络（`Network`）**中，实现零中央服务器的节点自发现、智能能力路由、入站权限门禁与本地工作区（如 Orca / Claude Code / Codex / Antigravity）自动唤醒派发。

---

## 核心特性

1. **加密多网络挂载（Multi-Network Mounting，`networkId + passphrase`）**
   - 团队即网络（`Network`）：无需冗余的 `team` 层级，团队成员约定 `networkId`（如 `sensebeing`）与共享口令 `passphrase`，本地通过 `HKDF-SHA256 / BLAKE3` 确定性派生出发现频道指纹 `swarmTopicId` 与帧加密准入密钥 `sealedKey`。
   - 单节点可同时挂载多个网络（如个人私域网 `ben-home` + 团队协作网 `sensebeing`），并为每个网络独立配置入站权限门禁与暴露的技能集。
2. **高性能 P2P 传输与大附件流（`transport`）**
   - 支持**定向单播（Unicast）**、**批量多播（Multicast）**、**千节点级频道广播（Broadcast）**。
   - 原生支持内容寻址的 **多媒体/大文件附件传输（`BlobAttachment`）**：截图、音视频、百兆 DebugLog 诊断包、Git Patch 按需流式拉取至本地 `0700` 隔离目录。
3. **节点身份与能力名片（`node` & `NodeCard`）**
   - 每个节点拥有 Ed25519 公钥身份（`nodeId`）、成员标识（`member`）、节点名（`nodeName`）以及类 Agent Skill 的自然语言+结构化标签能力描述（`skills`），随 Gossip 心跳实时同步负载状态（`idle | busy | overloaded`）。
4. **高自由度发送器与 AI 发送判断器（`emitter` & `router`）**
   - 外部触发源插件化：支持 Webhook、单水位轮询（Lark 告警 / 反馈认领 / 低质会话认领）、自定义脚本或节点内 Agent 直接调用。
   - 三级智能路由：显式身份直指（`member:oliver`）→ 技能标签匹配（`skill:prod-release`）→ **AI 发送判断器插件**（挂载本地 AGY / Claude Code / Codex 或 LLM API 读取在线节点 `NodeCard` 自动裁决单播/多播目标）。
5. **节点自主权限门禁（`gate`）**
   - 四态权限控制：`open`（直接放行）、`approval_required`（Knock 敲门待确认模式，确认后可一键签发发送方信任白名单）、`dnd`（免打扰仅入队）、`closed`（拒收）。
6. **插件化接收派发器（`dispatcher`）**
   - **调度策略插件**：内置 `policy-dedup-cas`（代次水位 CAS 防重）与 `policy-wave-coalescer`（单水位波次聚合 + 工作区空闲门禁 `Idle Gate` + 会话满阈值自动 `/new` 轮转）。
   - **终端执行器插件**：内置 `executor-orca-spawn`（自动新建 Orca 独立工作区并启动 Agent）、`executor-orca-wave`（投递常驻 Orca 工作区/Run 并更新卡片状态）、`executor-agent-pty`（无 Orca 环境的通用 CLI Agent 执行器）与 `executor-notify`。

---

## 详细设计文档导航

- [整体架构与六大核心模块契约](docs/architecture.md)
- [P2P 帧协议、多网络密钥派生与大文件流规范](docs/wire-protocol.md)
- [插件开发指南（Source / Router / Gate / Policy / Executor）](docs/plugin-guide.md)
- [配套 Agent Skill (`skills/the-dealer/SKILL.md`)](skills/the-dealer/SKILL.md)
