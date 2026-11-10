# The Dealer

**连接你的所有 Agent —— 跨工作区、跨机器、跨队友。**

The Dealer 把独立的 Agent 会话连接为协作网络，统一处理路由、权限和持久化收件箱。从一台机器上的零网络配置开始，再通过加密 P2P 连接自己的其他机器和队友，复用同一条投递链路。

```text
1  Same-Machine Mesh / 同机多 Agent
   Orca | Antigravity | Claude Code | Codex | ChatGPT Dot | Grok Bot
                              |
2  Cross-Machine Personal Mesh / 个人跨机网络
                 MacBook <-> Mac mini <-> Devbox
                              |
3  Cross-Teammate P2P Collaboration / 跨队友协作
   dealer-net:// -> Gate -> Durable Inbox -> spawn 新会话
                                         -> fixed 专属会话
```

[English](README.md) | 简体中文

## 30 秒接入 —— 复制提示词发给你的 Agent

### 提示词 1：在本机启动或加入 —— 粘贴给 Claude Code / Codex / Antigravity / Orca

```text
帮我配置 The Dealer，保留现有配置和 ~/.dealer/identity.json。

1. 检查 `command -v dealer` 和 `node --version`，要求 Node >=22.6。
   如果没有 dealer，将 https://github.com/sensebeing/the-dealer.git 克隆到
   ~/the-dealer。如果该检出已存在，先检查本地修改，再用
   `git -C ~/the-dealer pull --ff-only` 更新。然后运行：
   cd ~/the-dealer && npm install && npm link
   如果 dealer 已安装，确保此可信检出可供读取技能，不替换现有安装。
2. 创建 ~/.agents/skills/ 和 ~/.claude/skills/，将以下三个目录分别软链到其中，
   若 ~/.gemini/config/skills/ 存在也同样安装：
   ~/the-dealer/skills/the-dealer
   ~/the-dealer/skills/the-dealer-setup
   ~/the-dealer/skills/the-dealer-extend
   保留已有文件，先处理软链冲突再修改。
   阅读 ~/the-dealer/skills/the-dealer-setup/SKILL.md。
3. 运行 `dealer setup probe --json`，检查本机 Agent App 和节点状态。
   不覆盖 ~/.dealer/identity.json，不复制其他机器的私钥。
4. 问我想要：(1) 同机多 Agent 调度，(2) 连接自己的多台机器，还是
   (3) 加入或创建队友网络，然后按 setup 技能执行。
5. 接入队友时，询问我的成员名、Agent App（orca、antigravity、claude-code、
   codex、chatgpt-dot、grok-bot）及会话模式（spawn 每个请求新开会话、
   fixed 固定专属会话、inbox-only 手动处理）。生成：
   dealer setup binding-preset --host <app> --mode <mode> --json
   Grok 执行需补充 --target-bot <bot>。按 setup 技能审阅并应用返回的 binding
   和 executorConfig，项目根目录由接收端本地绑定。
6. 验证实际投递、收件箱读回和回复；确认对端身份前保持 approval_required。
```

### 提示词 2：邀请队友或自己的第二台机器 —— 生成并发给对方

创建者先在本机挂载网络，再生成新邀请：

```sh
dealer setup invite-prompt --network <network-id>
# 加 --json 获取独立的 crossMachinePrompt 和 teammatePrompt 字段。
# 可选：--peer-member <成员名> --name <另一节点名> --seed <可达主机:端口>
```

将 `crossMachinePrompt` 发给自己第二台机器上的 Agent，或将 `teammatePrompt` 发给队友。两者都包含安装、技能接入、网络挂载和验证步骤；没有显式或已配置 seed 时，自动嵌入运行中节点公布的 TCP 端点，离线且配置了固定端口时使用主 LAN IPv4。

完整可复制模板（执行前将 `<invite-uri>` 替换为生成的 `dealer-net://...` URI，并补齐其他占位符）：

```text
将这台机器接入我的 Dealer 网络，保留现有配置和身份。

检查 `command -v dealer`，确认 Node >=22.6。若未安装，将
https://github.com/sensebeing/the-dealer.git 克隆到 ~/the-dealer；已有检出则先
检查本地修改，再 `git -C ~/the-dealer pull --ff-only` 更新，然后运行：
cd ~/the-dealer && npm install && npm link
若已安装，确保可信的 ~/the-dealer 检出可用于读取技能。
创建 ~/.agents/skills/ 和 ~/.claude/skills/，将
~/the-dealer/skills/the-dealer、~/the-dealer/skills/the-dealer-setup、
~/the-dealer/skills/the-dealer-extend 分别软链到两处；
~/.gemini/config/skills/ 若存在也安装。保留无关文件，先解决软链冲突。
阅读 ~/the-dealer/skills/the-dealer-setup/SKILL.md，运行 `dealer setup probe --json`。
不覆盖 ~/.dealer/identity.json，不复制另一台机器的私钥。

询问这是我的第二台机器还是队友的机器：自己的机器复用成员名，队友则询问
其成员名，不把 teammate 当作真实身份。询问新节点名，然后运行：
dealer node up --member '<member-handle>' --name '<node-name>' --json
dealer network mount '<invite-uri>' --json

询问接收端 Agent App：orca、antigravity、claude-code、codex、chatgpt-dot、
grok-bot；以及模式：spawn、fixed、inbox-only。生成：
dealer setup binding-preset --host <app> --mode <mode> --json
Grok 执行需加 --target-bot <bot>。按 setup 技能审阅并应用预设，项目根目录
在接收端绑定，确认宿主 CLI 支持对应参数。确认对端身份前保持 approval_required。

dealer peers list --network '<network-id>' --json
dealer send --network '<network-id>' --to 'member:<inviter-handle>' --topic setup.ping --body 'Please reply to verify connectivity' --json
验证双向回复、持久化收件箱和宿主读回。dispatched 回执不等于任务完成。
跨机器成员名重复时，用具体的 member@node selector。
```

邀请包含共享密钥，只私下发给目标接收者，不提交到仓库。生成邀请是只读操作；创建者尚未挂载网络时，也要挂载同一份生成的邀请。TCP seed 支持可路由子网之间的发现，但需要地址和端口可达，不绕过防火墙，也不自动穿透 NAT；自动检测的 LAN 地址不合适时，用 `--seed` 指定 VPN 或可路由端点。

## 三层渐进能力

### 1. 同机多 Agent 调度

一个开发者、多个 Agent、独立工作区。运行 `dealer node up`，向自己的 `member:<成员名>` 或本地 skill 发消息，无需邀请、共享密钥或挂载网络；同机投递仍经过门禁、调度器、去重和执行器。

### 2. 个人跨机器网络

MacBook、Mac mini 和 devbox 可以使用同一个成员名，各自保留独立签名身份。挂载同一邀请后，通过本地注册目录、UDP 组播或显式 TCP seed 发现节点，用 `member@node` 精确寻址某台机器。

### 3. 跨队友加密 P2P 与持久化收件箱

分享网络邀请，由接收端决定准入和执行方式。持久化消息、投递租约、双向线程路由及可跨重启恢复的唤醒保证请求可读回；`ask`、`reply`、`settle` 将对话推进与执行器接收区分开。Orca 唤醒仅在权威空闲检查通过后注入消息指针。

## 支持的 Agent App 与会话模式

| Agent App（`--host`） | 集成方式 | 接收端配置 |
| --- | --- | --- |
| `orca` | 托管工作区 / 空闲门控会话 | `--agent codex`、`claude` 或 `antigravity`，本地工作区与项目 |
| `antigravity` | Orca 托管工作区中的 Antigravity | 接收端工作区；独立 `agy` 也可通过 agent-pty 使用 |
| `claude-code` | Claude Code CLI | 可选 profile 与固定 session ID |
| `codex` | Codex CLI | 可选 profile 与固定 thread/session ID |
| `chatgpt-dot` | OpenAI ChatGPT Personal Agent `dot` 桥接 | Agent/bot selector 与 session ID |
| `grok-bot` | Grok Bot 桥接 | 执行时必须指定 `--target-bot <bot>` |

- **`spawn`**：每个请求创建新会话，Orca 创建隔离工作区。
- **`fixed`**：固定专属会话承接持续协作；Orca 使用空闲门控工作区，其他宿主遵循其会话接口。
- **`inbox-only`**：仅持久化消息，供手动处理，不启动 Agent。

六种宿主都提供三种模式的预设。`binding-preset` 只输出方案，不写配置或开启执行；probe 检测安装情况不代表 CLI 兼容，需要确认本机宿主或桥接程序支持生成的参数。详见[宿主矩阵](skills/the-dealer-setup/references/agent-app-matrix.md)。

## 使用 the-dealer-extend 分三层扩展

1. **个人 Patch**：定制本机 profile、路由、门禁和执行器配置，无需修改项目。
2. **`.dealer/project.json` 项目插件**：声明项目身份、topic 和插件，将仓库关联到接收端本地路径。
3. **自定义 Executor / Router / Source**：通过类型化插件接入新宿主、路由策略、webhook 或水位采集源，内核统一负责生命周期和依赖注入。

从 [the-dealer-extend](skills/the-dealer-extend/SKILL.md) 开始，再读[插件指南](docs/plugin-guide.md)。零外部 npm 运行时依赖，Node.js >=22.6 直接运行 TypeScript 源码。

## CLI 速查

```sh
dealer setup probe --json
dealer node up --member alice --name laptop --json
dealer node status --json
dealer setup binding-preset --host codex --mode fixed --session-id <thread> --json
dealer network mount <network-id> --passphrase '<shared-secret>' --gate approval_required --json
dealer setup invite-prompt --network <network-id>
dealer peers list --network <network-id> --json
dealer send --network local --to member:alice --topic collab.request.v4 --body 'Review this change' --json
dealer inbox check --mailbox <mailbox-id> --json
dealer inbox ack <dlv-id> --mailbox <mailbox-id> --json
dealer thread show <message-id> --json
npm test
```

回复与结算流程参见[日常操作技能](skills/the-dealer/SKILL.md)。

## 架构与安全

```text
Sources / CLI -> Emitter -> Router -> Transport -> Gate -> Dispatcher -> Executor
                         Node identity & directory       Durable mailbox
                  Kernel: lifecycle, injection, typed events
```

Ed25519 签名身份与消息，HKDF-SHA256 派生网络独立密钥，AES-256-GCM 保护帧。默认传输使用 TCP、加密发现信标和流式 BLAKE3 文件校验；可选 Rust Iroh 引擎位于 `native/`。准入状态由接收端控制（`open`、`approval_required`、`dnd`、`closed`），路由决定限制在当前网络候选集内，发送端路径不能决定接收端项目根目录；外部执行结果未知时不盲目自动重放。

[架构](docs/architecture.md) · [线协议](docs/wire-protocol.md) · [接入技能](skills/the-dealer-setup/SKILL.md) · [MIT 许可证](LICENSE)
