# `the-dealer` — 单机多 Agent 协作与跨机/跨团队实时 Agent 协作网络架构与模块契约设计

> [!IMPORTANT]
> - **项目名称**：`the-dealer`（CLI：`dealer`，后台守护进程：`dealerd`）
> - **核心隐喻与工程模块对齐**：对外品牌采用 *"The Dealer — Deal the right task to the right agent, across any machine"*；内部工程模块严格采用清晰直观的工程命名：**`kernel` / `transport` / `node` / `emitter` / `router` / `gate` / `dispatcher`**。
> - **核心设计准则**：
>   1. **Everything is a Plugin（无特权内核）**：参考 DeepSeek Harness (`Cordis`)，核心能力全部表现为挂载在 `Context` 上的标准 `Service` 与 `Typed Events`，通过 `Profile + Patch` 声明式组装，支持热插拔（HMR）。
>   2. **单一真源（Single Source of Truth）**：节点身份、能力名片（`NodeCard`）、门禁策略、投递租约水位（`Watermark / Revision`）各自由唯一模块拥有，不跨层复制状态。
>   3. **信令与大数据流分离**：控制信封与实时协作消息走低延迟 QUIC Stream / Gossip 树；日志包、截图、音视频、Git Patch 走 `iroh-blobs`（BLAKE3 哈希树内容寻址流），零中心服务器支持千节点级低延迟分发。

---

## 零、 使用层级：Local First，P2P 渐进扩展

`the-dealer` 的第一使用形态是**单机多 Agent / 多工作区自动协作与派发中枢**：一名开发者即可在同一台机器上协调多个专用 Agent 工作区（dev/prod 告警波次、反馈与低质会话认领、prod-release 交接、隔离 Orca 子工作区任务）。当团队挂载加密协作网络后，同一套内核与插件契约再渐进扩展为**跨机器设备互联、跨团队成员 P2P 协作网络**。

### 为什么是 Local First？

绝大多数多 Agent 协作首先发生在同一台开发机上：多个 Orca 工作区、不同 Agent CLI、各类告警/反馈触发源共享同一个开发者与同一份本地代码。若这一层必须依赖网络挂载、口令分发、Peer 发现与远端可达性，单机任务会被无谓地拖入分布式复杂度。`the-dealer` 因此把单机回环作为第一等能力，同时保留与跨机协作完全相同的信封签名、门禁、幂等、波次聚合与执行器链路，避免形成第二套派发栈。

### 第一步 — 单机多 Agent 协作（本地优先，零网络配置）

- 运行 `dealer node up` 启动本机节点；无需 `dealer network mount`、`networkId`、`passphrase` 或任何外部 Peer。
- 将消息发往 `member:<self>`、`skill:<local-skill>` 或本机 `ed25519:<nodeId>`。`transport` 在目标为本机且网络未挂载时，直接在进程内把帧交给同一 `transport/inbound` 处理器；已挂载时则复用本地 endpoint。
- `node.resolveTarget` 优先返回已发现的远端节点，仅在无远端匹配时回退本机自身 `NodeCard`；`router` 将本机名片作为合法候选，`gate` 对本机签名身份放行 `approval_required`（`dnd` 仍入队，`closed` 仍拒收）。
- 项目清单中的 `defaultNetworkId` 只是其触发源在本机发送时使用的稳定作用域名称，不代表必须进行网络挂载。

### 第二步 — 跨机器 / 跨团队成员 P2P 协作（`networkId + passphrase`）

- 团队成员约定 `networkId` 与共享口令 `passphrase` 并执行 `dealer network mount`；`transport` 通过 HKDF-SHA256 派生 `swarmTopicId` 与 AEAD 帧密钥，完成加密发现、准入与网络隔离。
- 签名 `NodeCard`、多维选择器、三级路由、四态门禁、CAS 去重、波次聚合与全部执行器无需改动，即可在加密协作网络内工作。
- 跨机 P2P 是同一内核的渐进扩展，而不是另一套协议或第二真源。

```bash
# 第一步：单机多 Agent 协作（零网络配置）
./bin/dealer.js node up --profile personal-dev --member ben.xu --name mbp --skills agent-runtime --json
./bin/dealer.js send --network local --to member:ben.xu --topic collab.request.v1 --body '{"summary":"Run the local quality sweep"}' --json

# 第二步：跨机器 / 跨团队 P2P 扩展
./bin/dealer.js network mount sensebeing --passphrase "team-shared-secret" --member ben.xu --node mbp-main --json
./bin/dealer.js send --network sensebeing --to member:oliver --topic collab.request.v1 --body '{"summary":"Review the release"}' --json
```

---

## 一、 整体架构拓扑与数据流闭环

```mermaid
flowchart TB
    subgraph OutboundNode["发送端节点 (Sender Node)"]
        direction TB
        Src["Source Plugins<br/>(Agent CLI / MCP / Lark / Feedback / Webhook)"]
        Emit["ctx.emitter<br/>(构建标准信封 + 挂载 Blob 附件哈希)"]
        Route["ctx.router (Waterfall)<br/>1. Explicit ID 直指<br/>2. Capability Tag 匹配<br/>3. AI Judge Plugin (AGY/Claude/Codex/LLM)"]
        Src --> Emit --> Route
    end

    subgraph IrohMesh["ctx.transport (Iroh P2P Mesh Layer)"]
        direction LR
        Gossip["iroh-gossip (HyParView + PlumTree)<br/>1. NodeCard 自发现与状态广播<br/>2. Topic 频道千节点毫秒级广播"]
        Quic["iroh-net (QUIC 0-RTT/1-RTT)<br/>mDNS 局域网直连 + 跨网 NAT 穿透<br/>定向单播 (Unicast) / 批量多播 (Multicast)"]
        Blobs["iroh-blobs (BLAKE3 Content-Addressed)<br/>多媒体 / 截图 / DebugLog / Git Patch<br/>按需分块流式拉取"]
    end

    subgraph InboundNode["接收端节点 (Receiver Node)"]
        direction TB
        NodeSvc["ctx.node<br/>(Ed25519 身份验签 + Peer 名片簿)"]
        Gate["ctx.gate (Bail 短路门禁)<br/>open | approval_required (Knock) | dnd | closed"]
        Disp["ctx.dispatcher (入站调度与执行管道)"]
        
        subgraph DispPipeline["Dispatcher Plugins"]
            direction TB
            Pol["Policy / Mailbox Plugins<br/>- dedup-cas (代次水位防重)<br/>- MailboxStore + WakeupRedriveService (持久化波次、Idle Gate、单行指针、会话轮转)"]
            Exec["Executor Plugins<br/>- first-class: inbox; Desktop Apps claude-app/chatgpt-app/antigravity-app; Terminal CLIs claude-code/codex-cli/antigravity-cli; orca<br/>- mode: spawn / wave / fixed / inbox-only<br/>- legacy aliases: orca-spawn / orca-wave / agent-pty<br/>- executor-notify (桌面/飞书交互审批)"]
            Pol --> Exec
        end

        NodeSvc --> Gate --> Disp
    end

    Route == "签名 Envelope + Blob Ticket" ==> IrohMesh
    IrohMesh == "验证通过帧" ==> NodeSvc
    Disp -. "ACK / Receipt (Admitted -> Queued -> Dispatched -> Settled) / Reply" .-> IrohMesh
```

---

### 四层工程拓扑与组合根

依赖上限及精确模块边由 `.guard/config.json` 共同强制执行。即便层级允许，未列入模块 `dependencies` 的导入仍被拒绝；`guard topology audit --json` 对全仓检查未登记模块、缺失文档与过期依赖。

| Layer | Modules | Allowed dependency layers |
| --- | --- | --- |
| foundation | `core-types`, `process-lock`, `fs-atomic`, `subprocess`, `kernel`, `native/dealer-iroh-engine` | foundation |
| core | `transport`, `node`, `router`, `gate`, `dispatcher`, `emitter`, `mailbox` | foundation, core |
| plugin | `plugins-policy`, `plugins-router`, `plugins-source`, `plugins-executor`, `plugins-agent-host`, `plugins-orca`, `plugins-lark-notifier` | foundation, core, plugin |
| cli | `cli`, public export facade `packages/index.ts` | foundation, core, plugin, cli |

`HostPluginRegistry`（`packages/cli/registry.ts`）组合内置 executor/policy 工厂。它从宿主插件的 `AGENT_HOST_DESCRIPTORS` 派生命名宿主执行器和 setup 映射，调用 Orca 插件的绑定校验、mode/stream 适配、wake target 同步与 session pinning hooks。CLI 不再维护第二份 executor/policy 白名单。`toMailboxTopicBinding` 是默认 registry 的兼容入口，保留 payload-aware `agent-pty` 与显式 mode 优先级。

外部 project/user 插件通过 `ctx.services.dispatcher.registerExecutor/registerPolicy` 注册，和内置执行器共用同一活跃注册表与附件包装；可在 `executors`、`defaultExecutor`、`executorConfig`、`policies`、`topicBindings` 中引用。配置含外部来源或 executor overrides 时只预校验标识符，全部插件加载后逐一核对实际注册，未注册即拒绝启动。没有外部来源的独立配置仍拒绝未知名字。

`dispatcher/inbound` 首先使用单目标执行回执的 authoritative `mailboxId`，缺省时通过 payload-aware registry 适配和 mailbox 的地址解析恢复；已交付 session 归属由插件 hook 与 `MailboxStore.pinSessionMailbox` 原子维护。`TopicBindingConfig.metadata` 是唯一开放的非空字符串注解映射；顶层仅接受已声明字段及显式兼容的 `assignee`，未知字段被拒绝，注解不赋予核心 runtime 业务语义。

`lark-mirror` 在插件内部创建 `LarkMailboxNotifier`，用 `ctx.provide('larkNotifier', ...)` 发布服务并用 `ctx.effect` 启停。Kernel unload/replacement 自动清理订阅与重试；runtime 通过只读 service getter 更新默认 mailbox 集。

## 二、 微内核与插件协议设计 (`@the-dealer/kernel`)

完全对齐 DeepSeek Harness (`Cordis`) 的五项核心原语，保证架构极简且具备无限扩展能力：

### 1. 插件协议契约 (`Plugin & Service Contract`)
每个模块或扩展插件都遵循统一的声明周期接口，通过 `inject` 声明依赖，通过 `ctx.effect()` 注册可逆副作用（卸载或配置热更新时自动清理定时器、监听器、子进程与网络连接）：

```typescript
export interface Plugin<C = unknown> {
  name: string;
  /** 显式声明依赖的服务名，内核按依赖拓扑自动等待并激活 */
  inject?: readonly (keyof Services)[];
  /** 插件挂载入口；返回清理函数或内部通过 ctx.effect() 注册可逆副作用 */
  apply(ctx: Context, config: C): void | (() => void | Promise<void>);
}

export interface Services {
  transport: TransportService;   // 底层 P2P 通信与 Blob 传输 (Iroh)
  node: NodeService;             // 本机身份、能力名片与全网 Peer 目录
  emitter: EmitterService;       // 节点内消息发送器与触发源管理
  router: RouterService;         // 发送目标判定器（显式 / 标签 / AI Judge）
  gate: GateService;             // 入站权限控制与敲门审批状态机
  dispatcher: DispatcherService; // 入站调度策略（水位/波次/空闲门禁）与执行器派发
}
```

### 2. 四种类型化事件派发模式 (`Typed Events`)
内核事件总线严格区分四类派发语义，杜绝隐式副作用：

| 事件名称 | 派发模式 | 触发时机与插件职责 |
| :--- | :--- | :--- |
| `node/peer-updated` | `emit`（只读广播） | 发现新节点、节点名片（能力/负载/权限状态）变更或离线时触发 |
| `emitter/prepare` | `waterfall`（洋葱中间件） | 消息发出前依次执行：注入发送方身份 → 打包本地文件为 Blob Ticket → 调用 `router` 解析目标节点列表 |
| `gate/evaluate` | `bail`（短路裁决） | 收到入站消息时优先执行：按规则返回 `allow`、`queue_approval`（待确认）、`defer_dnd`（仅入队）或 `reject`；一旦非 `allow` 立即短路 |
| `dispatcher/route` | `serial`（有序派发） | 通过 `gate` 的消息先经 `PolicyPlugin`（CAS 去重），就绪后交由匹配的 `ExecutorPlugin` 落地执行并回传 `DeliveryReceipt`；非 `rejected` / `failed` 的放行结果统一触发 `dispatcher/inbound`，驱动信箱反向路由与持久化唤醒 |

### 3. `Profile + Bundle + Patch` 声明式配置
与 `dsh` 一致，节点运行时是一棵由 YAML 组装的插件树，保存在 `~/.dealer/profile.yaml`（并支持通过 Agent Skill 写入 `patch.yaml` 实时热更新）：
- **内置 Profile 模板**：
  - `personal-dev`（个人研发节点：开启 mDNS + Team Swarm、Orca Spawn/Wave 执行器、Claude/Codex/AGY 路由判断器、默认门禁为 `open`，可通过 `--gate approval_required` 开启入站审批）
  - `dedicated-worker`（专属自动化节点，如你的 `prod-release`、`alert-processor`、`ios-crash`：对团队认证成员 `open`，通过持久化 `MailboxStore` + `WakeupRedriveService` 实现波次恢复、权威空闲门禁与会话自动轮转）
  - `headless-relay`（纯命令行或服务器节点：无 GUI 依赖，使用 `agent-pty` 执行器）

---

### 通用进程工具与 Orca 适配边界

| 模块 | 唯一职责 | 非职责边界 |
| :--- | :--- | :--- |
| [`packages/subprocess/index.ts`](../packages/subprocess/index.ts) | 零内部依赖的 `CommandRequest` / `CommandRunner`、`CommandNotStartedError`、`runCommand`、`checkedCommand`、`findExecutable` 与 `singleLine` | 仅提供通用 OS 子进程、PATH 查找与单行文本工具；不依赖其他内部包，不决定业务执行策略 |
| [`packages/plugins-orca/index.ts`](../packages/plugins-orca/index.ts) | Orca 工作区/终端空闲检查、终端解析、受控投递与工作区备注 | 使用 `subprocess` 执行命令，不重新导出通用进程工具；非 Orca 调用方直接依赖 `subprocess` |

`runCommand` 始终使用 `shell: false` 参数数组；确认进程未启动时抛出 `CommandNotStartedError`，非零退出仍拒绝并携带 `stdout`、`stderr`、`exitCode`。守护进程代码指纹递归覆盖 `packages/`，因此自动包含 `subprocess`。

## 三、 六大核心工程模块详细设计与接口契约

### 模块 1：`transport` — 基于 Iroh 的多网络挂载与 P2P 传输底座 (`@the-dealer/transport`)

职责：**负责「协作网络（`Network`）」的密码学派生与准入握手、同网络节点零配置自发现（mDNS + DHT/Gossip）、以及单播/多播/千节点广播与大文件 Blob 的点对点传输**。

#### 1. 核心设计：什么是 `Network`（协作网络 / 牌桌）？
遵循最简架构与唯一真源原则，**不再单独设立冗余的 `team` 字段——一个协作团队（或个人私域空间）在架构上就是一个 `Network`（牌桌）**，且**一个节点可以同时挂载到多个 `Network`**（如同时挂载个人网络 `ben-home` 与公司团队网络 `sensebeing`）。

团队成员只需约定两个要素即可将各自机器挂载到同一个网络：**`networkId`（网络名称，如 `sensebeing`）+ `passphrase`（团队共享口令/密钥）**。

底层通过 `HKDF-SHA256 / BLAKE3(networkId, passphrase)` 确定性派生出三把核心密钥，实现**无中央服务器的安全自发现与网络隔离**：
1. **`swarmTopicId`（32 字节 Gossip 与发现频道 ID）**：
   - **同局域网（mDNS）**：广播 `swarmTopicId` 的不可逆哈希指纹（外人无法嗅探出明文网络名 `sensebeing`），同一 Wi-Fi/内网下持有相同口令的队友机器在 100ms 内自动识别并直连。
   - **跨公网无中心会合（DHT / Pkarr Rendezvous）**：由口令派生出确定性的虚拟会合槽位（Rendezvous Keypair），挂载到该网络的节点将自己加密后的 `(nodeId, relayUrl, directAddrs)` 宣告至 DHT 会合点并通过 `iroh-gossip` 流行病树扩散——**即使异地办公、无中心服务器、不手工抄写对方公钥，只要对上 `networkId + passphrase`，就能在公网上自动找到同网络的所有队友节点！**
2. **`networkSealedKey`（32 字节 AEAD 对称加密与准入密钥）**：
   - 该网络内的所有 Gossip 广播（含 `NodeCard` 名片交换）与 QUIC 握手证明均使用 `networkSealedKey` 加密认证。**没有正确口令的外部节点不仅在网络上搜不到你们的牌桌，即使知道你的物理 `NodeId` 也无法向该网络注入任何伪造消息。**
3. **支持可选种子节点加速（Bootstrap Hint）**：
   - 虽然通过 `networkId + passphrase` 即可经 mDNS / DHT 自动聚拢，命令仍支持可选传入 `--bootstrap <peer_node_id>`（或导出 `dealer://sensebeing?secret=...&bootstrap=...` 一键邀请码）实现亚秒级冷启动入网。
4. **无 NAT 穿透时的密封中继（Self-hosted headless relay）**：
   - **连通模式**：同网段靠 UDP `239.255.42.99:45873` 组播与子网广播发现；装有 Tailscale 的机器把加密信标单播到 `tailscale status` 中每个在线节点的 UDP 45873 并走 `100.64.0.0/10` 直连；两者都不满足时，由一台常驻 `headless-relay` 节点充当种子。
   - **信标 Gossip**：`announce` 把本机已收集的**密封**发现帧（`beacons`，按 `senderNodeId` 只保留最新有效帧，过期超过 30s 清理）随 `discovery` 探测发给 `bootstrapPeers`，种子回送自己的帧与全部已知信标。节点据此学习对端 endpoint 以及 `relays`（哪个种子可以把帧送到该 peer）。入站探测携带的第三方 peer 帧按探测发送者的实际 endpoint 记为 `via: relay`，不会被误标为 direct；本机自己的帧一律跳过。`m.relays` 只记录**经由种子可达的第三方 peer**，绝不把种子自身映射到它的直连 endpoint；`relayCandidates` 也跳过等于目标自身 endpoint 的种子，因此稳态种子探测不会制造虚假的可达转换。
   - **常驻隧道**：每个挂载网络对每个种子保持一条 `tunnel` 长连接（`keepAlive(15s)`，无空闲超时），种子以 `peerNodeId -> 隧道` 登记。即使对端只宣告了不可直连的 NAT 地址，种子也能沿隧道把 `relay_deliver` 推回去，对端的 `relay_reply` 再沿原路返回。
   - **自身端点排除**：`bootstrapPeers` 中解析到本机监听端口的任意别名（本机 endpoint、`127.0.0.1`、任一网卡地址、Tailscale 地址）都由 `isSelfEndpoint` 静态过滤；无法静态识别的 hairpin 转发器（不同端口的反向代理）由线协议兜底：收到携带本机 `senderNodeId` 的密封 `relay`/`relay_send` 帧时回 `{ selfRelay: true, selfProof }`。`selfProof` 是对 `purpose + 请求 nonce` 的进程内 HMAC，且仅当该 nonce 正在本进程 in-flight 等待应答时才签发（发现与 relay 域分离），因此网络密钥持有者即使伪造本机 `senderNodeId` 也拿不到可重放的 proof；不在 in-flight 集合内的自环 relay 帧直接以 `Invalid relay sender` 拒绝。发送端验证通过后才 `recordSelfEndpoint` 并继续尝试下一个真实种子，绝不向自身建隧道或把自己的帧暂存进自己的 relay-spool；验证失败则是确定性 `LocalDeliveryError`，绝不静默转成离线暂存。发现应答的自环短路由同样要求对应域的 `selfProof`，任意 TCP 服务器仅凭回显本机 `senderNodeId` 无法拉黑真实种子。挂载时，快照与 journal 中 `senderNodeId === localNodeId || targetNodeId === localNodeId` 的历史条目直接丢弃并压缩进新快照。
   - **可达性与超时归因**：`transport.isPeerReachable(networkId, nodeId)` 以未过期的密封信标为唯一判据；半开僵尸 tunnel（对端合盖未发 FIN，信标已过期但 socket 尚未被 OS 回收）不构成存活证明，因此 `learnWithMeta` 在信标过期后收到新信标时总是发出 `transport/peer-reachable`，即使旧 tunnel 仍在表内。`transport/peer-reachable` 仅在真实转换（由暗变亮，或证明路由改变）时发出，携带 `via: direct | relay | tunnel`，与 `NodeCard.revision` 无关，因此睡眠唤醒/仅经种子 gossip 重连都能立即触发重驱，而稳态信标刷新保持静默；新 tunnel 建立时仍按 `via: tunnel` 发出一次。中继超时按信标证据归因：只有在目标仍有新鲜信标时，已建连但静默的 socket 才是 `DeliveryTimeoutError`（可能已执行）；目标无新鲜信标时一律是 `ETIMEDOUT` 连接失败。发送端遇到信标已过期的 inbound tunnel 时把该次尝试压缩到 `RELAY_CONNECT_TIMEOUT_MS` 内，快速淘汰僵尸 tunnel 并让后面的真实种子接手；转发进半开僵尸隧道的 `relay_deliver` 同样带自身 deadline，超时即销毁该隧道并落入暂存，绝不无限等待外层请求。
   - **盲转发**：`exchange` 先直连；存在中继候选时直连只等 1500ms，失败后按用途选择线协议动词：普通实时转发发送 `relay`（与升级前的种子兼容），只有携带 `spoolIfOffline` 的暂存报价才发送 `relay_send`；种子同时接受 `relay` 与 `relay_send`。种子只校验该信封确实属于已挂载网络的 AEAD 密钥（不解析业务载荷），按目标隧道或直连转发。附件拉取同理：直连失败后发 `relay_blob`，由种子向目标按 BLAKE3 内容取回并回传，单次中继载荷上限 `MAX_FRAME - 4096`。发送端是否附带附件不以“原始字节总和”估算，而是按实际序列化后的 relay 包（密封帧、JSON 键名、逐哈希 base64 膨胀与逗号）判断；一旦完整包会超过 `MAX_FRAME`，该次发送明确降级为 live-only（`complete:false`），绝不把部分附件塞进一个必然被 `packet()` 拒绝的帧。未升级种子收到 `relay_send` 会直接断开连接（客户端表现为 `Truncated stream`），因此该快速失败先由信标证据裁决：若目标仍有未过期信标且本次交换预算尚有余量，说明其在线（典型是 NAT 后的存活节点），发送端立即改用 `relay` 动词重试以完成实时投递；无新鲜信标时才把这次快速失败判定为真实离线，转为 `PeerUnreachableError`（`inDoubt:false`）并转入本地 outbox，而不是等待完整超时。
   - **离线暂存与自动排空**：`relay_send` 可携带 `spoolIfOffline`、调用方生成的稳定 `spoolKey` 与附件 BLAKE3 哈希。种子先对信封做 AEAD 成员校验（解封失败即拒绝），再逐个校验附件内容并按内容哈希写入**网络隔离**的附件目录；暂存条目只保存 `id`、密封帧与 `blobHashes` 元数据，不在 JSON 快照里重复 base64 正文。目标必须是 `ed25519:` NodeId 且不能是种子自身；条目以 `${senderNodeId}:${spoolKey}` 幂等，发送方不同者永远不能覆盖同一键，容量保护/淘汰始终按唯一的 `(targetNodeId, id)` 对判定，同一 `spoolKey` 跨目标不会互相豁免。总量同时计算**序列化后的密封帧字节**与附件字节，受每目标 256 条、每发送方 256 条、全局 1024 条与 32MiB、目标数 128 等边界约束，超限按最旧优先从违规的发送方/目标连同附件文件一起回收，加载快照时同样执行全部边界。运行时入队/排空只向 append-only journal 追加单条 `upsert`/`remove`/`drain` 记录，超过 32MiB 或启动/关闭时再合并为一次原子快照，避免每次入队同步重写整份 spool 阻塞种子。默认 TTL 7 天。目标重新建立隧道或刷新信标后，种子按序 `relay_deliver` 并在收到绑定 `requestNonce` 的回执后删除帧条目，但附件缓存保留到原 TTL，确保目标在回执之后才拉取附件、且原发送者已离线时仍可回答 `relay_blob`。`fetchBlobToPath` 在直连与目标中继都失败后还会回退到各挂载网络的 bootstrap 种子。
   - **离线收件人仍是成员校验后的暂存**：暂存不绕过网络隔离，种子只接受已挂载网络、且能被该网络 AEAD 密钥解封的信封；附件按内容哈希校验，任何不一致都会拒绝暂存而不是把损坏内容交给接收端。
   - **地址宣告**：`--advertised-host <host>`（或 `DEALER_ADVERTISED_HOST`）覆盖自动探测的 LAN 地址，供反向代理、端口映射或多网卡主机使用。

```typescript
export interface NetworkMountConfig {
  networkId: string;             // 网络可读标识，如 "sensebeing" 或 "ben-home"
  passphrase: string;            // 团队约定的共享口令（本地派生密钥，绝不明文上链/外发）
  bootstrapPeers?: readonly string[]; // 可选种子节点 NodeId，加速跨公网冷启动会合
  admissionMode?: AdmissionMode; // 该网络专属的默认入站门禁（如个人网 "open"，团队网 "approval_required"）
  exposedSkills?: readonly string[];  // 可选：仅向该网络暴露本机的部分特定技能（不填则暴露全部）
}

export interface DerivedNetworkKeys {
  networkId: string;
  swarmTopicId: string;          // BLAKE3/HKDF 派生的 32B Gossip & mDNS 发现 Topic ID (hex)
  rendezvousPublicKey: string;   // 跨网 DHT 会合槽位公钥
  sealedKey: Uint8Array;         // 32B AEAD 帧加密与握手准入密钥
}

export interface BlobAttachment {
  name: string;                  // 文件名，如 "crash-report.jsonl.gz" 或 "api-contract.patch"
  mimeType: string;              // MIME 类型，支持 application/octet-stream, image/png, audio/* 等
  sizeBytes: number;
  blake3Hash: string;            // Iroh BLAKE3 内容寻址哈希
  ticket: string;                // Iroh BlobTicket，接收端凭此直接通过 QUIC P2P 分块拉取
}

export interface TransportService {
  readonly localNodeId: string;  // 本机 Ed25519 公钥编码 (NodeId)

  /** 挂载/卸载协作网络（支持单节点同时挂载多个 Network） */
  mountNetwork(config: NetworkMountConfig): Promise<MountedNetworkHandle>;
  unmountNetwork(networkId: string): Promise<void>;
  listMountedNetworks(): readonly string[];

  /** 1. 定向单播 (Unicast)：在指定 Network 域内通过 QUIC 双向流直接发送并等待回执 */
  sendUnicast(networkId: string, targetNodeId: string, frame: Uint8Array, opts?: { timeoutMs?: number }): Promise<Uint8Array>;

  /** 2. 批量多播 (Multicast)：并发复用到多个目标节点的 QUIC 连接池 */
  sendMulticast(networkId: string, targetNodeIds: readonly string[], frame: Uint8Array): Promise<Map<string, Result<Uint8Array>>>;

  /** 3. 网络内广播 (Broadcast)：基于 iroh-gossip (HyParView + PlumTree) O(log N) 流行病广播树 */
  broadcast(networkId: string, subTopic: string, frame: Uint8Array): Promise<void>;

  /** 4. 多媒体与大文件流 (iroh-blobs)：本地文件零拷贝挂载为内容寻址 Blob，接收端按需拉取 */
  shareFile(filePath: string): Promise<BlobAttachment>;
  fetchBlobToPath(ticket: string, destPath: string): Promise<void>;
}
```

---

### 模块 2：`node` — 节点身份、多网络挂载名片与全网目录 (`@the-dealer/node`)

职责：**管理本机节点身份（`Identity`）与能力描述（`NodeCard`），并按已挂载的 `Network` 维护各网络内活跃队友节点的实时目录（`PeerDirectory`）**。

```typescript
export interface SkillCapability {
  id: string;                    // 如 "ios-crash-triage", "prod-release", "agent-runtime-review"
  description: string;           // 供 AI Router Judge 阅读的自然语言能力边界描述
  tags?: readonly string[];      // 供确定性路由匹配的标签，如 ["ios", "swift", "dsym"]
  acceptedTypes?: readonly string[]; // 接收的消息类型，如 ["alert.crash", "collab.request"]
}

export interface NodeCard {
  nodeId: string;                // Ed25519 公钥 (唯一加密身份)
  networkId: string;             // 当前名片所属的协作网络 ID（如 "sensebeing"）
  revision: number;              // 单调递增名片版本号（防旧状态回退）
  identity: {
    member: string;              // 成员标识，如 "ben.xu", "oliver", "diz"
    nodeName: string;            // 机器/节点名，如 "mbp-harlequin", "seabass-release"
  };
  admissionMode: AdmissionMode;  // 该节点在当前 Network 下的权限状态："open" | "approval_required" | "dnd" | "closed"
  capabilities: {
    summary: string;             // 节点整体职责简介
    skills: readonly SkillCapability[];
  };
  runtimeState: {
    load: "idle" | "busy" | "overloaded";
    activeTasks: number;
    availableAgents: readonly string[]; // ["antigravity", "claude", "codex"]
  };
  updatedAt: string;             // ISO-8601
  signature: string;             // NodeId 私钥对上述字段的签名，防网络内成员伪造他人身份
}

export interface NodeService {
  getSelfCard(networkId: string): NodeCard;
  updateSelfProfile(patch: Partial<{ member: string; nodeName: string; capabilities: NodeCard["capabilities"] }>): Promise<void>;
  /** 按 Network 列出已发现的活跃节点名片 */
  listPeers(filter?: { networkId?: string; member?: string; skillTag?: string; onlyAvailable?: boolean }): readonly NodeCard[];
  /** 支持跨网或指定网解析选择器，如 "sensebeing/member:oliver", "ben.xu@mbp", "skill:prod-release" */
  resolveTarget(selector: string, defaultNetworkId?: string): readonly NodeCard[];
}
```

- **已验证目录持久化**：配置 `peersPath` 后，每张通过 `verifyNodeCard` 的远端名片与本机每网络最后 revision 会原子写入 `peers.json`（临时文件 + `fsync` + `rename` + 父目录 `fsync`，`0600`），启动时重新逐张验签加载。这样即使队友离线，`member:` / `skill:` 选择器仍能解析出稳定目标，出站消息可进入持久化 outbox 等待重驱，而不是因为目录为空而路由失败。本机名片本身不落盘；下一张名片 revision 取 `max(持久化/已知 + 1, 当前毫秒时间)`，因此重启后的名片必然严格更新，不会被队友按旧 revision 丢弃。
- **名片 Gossip**：`peer.card` 广播除自身名片外还携带 `knownCards`（最多 128 张已验证远端名片）。重启节点或只能经种子入网的新节点因此能在队友重新上线前拿到团队目录；接收端仍逐张验签、按 `revision` 单调更新并落盘，旧名片或伪造名片不会覆盖已有记录。临时卸载网络只清除本机名片，已验证远端目录保留。

---

### 模块 3：`emitter` — 节点内消息发送器与触发源扩展 (`@the-dealer/emitter`)

职责：**提供极高自由度的消息构造与发送入口，向下统一转换为带 `networkId` 作用域的标准 `MessageEnvelope`，支持外部事件源插件（`SourcePlugin`）与本地 Agent 直接调用**。

```typescript
export interface MessageEnvelope<T = Record<string, unknown>> {
  id: string;                    // 全局唯一消息 ID (ULID/UUIDv7)
  networkId: string;             // 所属协作网络 ID（如 "sensebeing"）
  traceId: string;               // 跨节点协作会话线程 ID（支持 ask / reply 多轮交互）
  replyToId?: string;            // 回复的具体消息 ID
  idempotencyKey?: string;       // 业务幂等键，如 "feedback:dev:7778ef78:v2" 或 "alert:ae_123"
  from: {
    nodeId: string;
    member: string;
    nodeName: string;
  };
  routing: {
    mode: "unicast" | "multicast" | "broadcast" | "auto";
    targets?: readonly string[]; // 显式目标（NodeId / "member:xxx" / "skill:xxx"）；mode="auto" 时可留空交由 Router 判定
    topic?: string;              // mode="broadcast" 时的网络内子频道
    hint?: string;               // 给 Router Judge 的自然语言路由提示，如 "发给负责运行时且当前空闲的队友"
  };
  kind: string;                  // 动态消息类型，如 "task.dispatch" | "collab.request" | "alert.event" | "agent.ask" | "agent.reply"
  priority: "urgent" | "high" | "normal" | "low";
  subject: string;               // 一句话标题
  body: string;                  // Markdown 正文或 Prompt 指令
  payload: T;                    // 任意结构化扩展载荷（由各业务插件自定义 Schema）
  attachments: readonly BlobAttachment[]; // 多媒体/文件/日志包/Git Patch 列表
  createdAt: string;
  signature: string;             // 发送端 Ed25519 签名
}

export interface EmitterService {
  /** 核心发送接口：支持本地文件路径自动转 BlobTicket、自动在指定 networkId 内触发 Router 解析目标并发送 */
  emit<T>(input: EmitMessageInput<T>): Promise<EmitResult>;

  /** 注册外部事件触发源插件（如 Lark 告警轮询、Feedback 认领唤醒、Webhook、Cron） */
  registerSource(source: SourcePlugin): Disposable;
}
```
- **内置与可扩展的 `SourcePlugin`（把 `alter-service` 触发源彻底插件化）**：
  - `plugin-source-cli-mcp`：供节点内正在运行的 Agent（AGY / Claude Code / Codex）通过 CLI 或 MCP 直接发消息。
  - `plugin-source-webhook`：本地安全 HTTP 端点（如承接 Source Hub `/feedback/wake` 或 GitLab/GitHub 事件）。
  - `plugin-source-poll-watermark`：通用水位轮询触发器基类，直接承载你的飞书多维表格告警源、Source Hub 反馈认领源、Runtime Monitor 低质会话源。
- **离线出站（Durable Outbox）**：`sendUnicast` 只有遇到可重试的连接类失败（允许列表内的 `ECONNREFUSED/RESET/TIMEDOUT/EHOSTUNREACH/ENETUNREACH/EHOSTDOWN/ENETDOWN/ENONET/ESHUTDOWN/DNS/截断流/超时`，或被种子 `spoolIfOffline` 接收后的 `OfflinePeerSpooledError`）才交给 outbox；其中 `EHOSTDOWN`/`ENETDOWN`/`ENONET`/`ESHUTDOWN` 覆盖 Darwin/BSD 同网段 ARP 失败与本地接口切换，Node Happy Eyeballs 抛出的 `AggregateError` 仅当全部成员都是连接类失败时才整体归类为离线。签名不符、回执伪造、密码学校验失败、缺失/损坏附件、畸形中继响应、中继 `networkId` 不匹配与帧超限等确定性本地/协议错误仍然直接抛给调用方，绝不伪装成“已排队”。网络被卸载是瞬时运行态：该网络上的义务保持 `queued` 并在重新挂载后继续重驱，而不是逐目标标记 failures。首次发送与后续重驱共用 emitter 内同一个逐目标发送/分类通道：`dispatched` / `settled` / 持久 `queued` / dedup 重复回执才是 delivered；`rejected` / `failed` 是该目标的终态签名证据；`pending_approval`、内存态 `queued`、`received` 等非终态仍属于待重驱义务。某个目标失败不会中断或丢弃同批其他目标；若确定性错误与其他目标的离线义务同批出现，emitter 先把离线目标及其 queued 回执原子写入 outbox，再把该确定性错误抛给调用方。TCP 三次握手未完成时的 socket 超时（零应用字节可能离开本机）按 `ETIMEDOUT` 连接失败处理，不会标记 in-doubt；只有已建连并写出帧后等待响应超时才使用 `DeliveryTimeoutError` 并记入 `inDoubtNodeIds`。中继链路上的超时按信标证据归因：目标没有新鲜信标时，连接种子或僵尸隧道的超时只是离线可达性失败（`ETIMEDOUT`），绝不能伪装成目标执行超时。in-doubt 目标在普通定时器上仍保持暂停，只有 `transport/peer-reachable` 的真实可达脉冲、直连 peer 脉冲、`transport.isPeerReachable` 证明目标在线后的常规退避重试，或显式 `--force` 才会用同一稳定 envelope id 重发；接收端 `DeliveryLedger` / `dedup-cas` 对重复信封幂等裁决，不会重复执行。
  - `emitter/sent` 是唯一的“发送已承诺”信号：只有广播成功、目标返回真实交接回执（含非终态 live 回执），或该目标已形成持久 outbox 义务时才发出；因此未入队的确定性失败与全部终态 `rejected`/`failed` 的发送不会在 `mailbox.json` 留下孤儿 `outbound:` 线程记录。多播中部分目标已承诺、部分目标确定性失败时，事件只携带已承诺的目标集合。
  - 配置 `outbox` 时，`emitter` 是唯一的出站发送、路由与回执校验 owner：首次发送和后续重驱都经过同一个 `redriveOutboxItem` 通道；`mailbox/outbox` 只持久化义务、退避调度与解析后的目标，不构造线协议帧、不路由、不解析回执。它把已签名信封与附件副本原子写入 `outbox.json`，并返回一张本机签名的 `stage: "queued"` 回执，`executionReceipt` 标记 `executor: "outbox"`、`queuedOffline: true`，若种子已暂存另带 `relaySpooled: true`。
  - 多播/部分目标离线时，已确认目标的真实回执照常返回，只为不可达目标追加 queued 回执；显式目标暂时无法解析（目录中尚无该成员）同样可以入队等目录恢复。
  - 调用方显式传 `queueIfOffline: false` 时，既不入本地 outbox，也不请求种子暂存（`spoolIfOffline` 不发送），不会留下任何可重放的持久副本。queued 回执是持久义务的确认，不是外部执行确认。
  - 同一 `(networkId, idempotencyKey)` 的并发/重复 emit 由 `OutboxStore.claimIdempotency` 在 outbox 文件锁内原子创建或加入**仍在 queued 的**唯一活动义务；并发 emit 不会都看到“不存在”而各自生成不同信封，只有 claim 成功者执行首次发送，其余调用方返回同一信封与持久 queued 回执。已经 delivered/failed 的历史义务不会被重新当作未决结果复用：再次使用同一业务键会发起新的发送尝试，由接收端 `dedup-cas` 按水位裁决是否重复。`executor: "outbox"` 的持久 queued 回执同时被 poll-source 水位视为已确认交接（与 settlement mode 无关）。
  - 需要离队的每个 `attachments` 附件都必须通过 `attachmentPaths[blake3Hash]` 提供本地源路径；emitter 会在发送前用 `shareFile` 校验哈希/大小并把副本复制进 `outbox-blobs/`。缺失路径、路径内容与声明不符，或 `emitter/prepare` 钩子注入了无法暂存的附件时，queueing 直接被拒绝，绝不生成重启后无法服务附件的 queued 信封。重驱前还会确认每个信封附件都有暂存副本，缺失即终态本地失败并保留证据。

---

### 模块 4：`router` — 发送判断器与智能路由模块 (`@the-dealer/router`)

职责：**在 `emitter` 发送消息时运行，将未绑定具体物理 `nodeId` 的消息，精准解析为 1 个或多个目标节点的 `NodeId`**。

支持**三级可插拔路由判定链（Matcher Pipeline）**：

```typescript
export interface RouteDecision {
  mode: "unicast" | "multicast" | "broadcast";
  targetNodeIds: readonly string[];
  matchedBy: string;             // 命中的路由器插件名，如 "explicit-id" | "capability-tag" | "judge:claude-code"
  reason: string;                // 可审计的路由理由
}

export interface RouteJudgePlugin {
  name: string;
  priority: number;
  /** 给定待发消息与当前全网活跃节点的 NodeCard 列表，返回路由裁决；返回 null 则交由下一级插件 */
  decide(envelope: MessageEnvelope, candidates: readonly NodeCard[]): Promise<RouteDecision | null>;
}
```
1. **Level 1 — `ExplicitSelectorJudge`（确定性显式寻址，内置）**：
   - 当 Agent 已经通过 Skill 查看了节点列表并指定了 `targets: ["oliver@mbp"]` 或 `targets: ["member:diz"]`，毫秒级直接解析为对应 `NodeId`。
2. **Level 2 — `CapabilityRuleJudge`（标签与技能规则匹配，内置）**：
   - 当消息指定了 `targets: ["skill:prod-release"]` 或 `payload.area == "agent-runtime"`，自动筛选具备该 Skill 且 `admissionMode != "closed"`、负载最优的节点。
3. **Level 3 — `AgentLLMJudgePlugin`（AI 智能发送判断器插件，核心亮点！）**：
   - 当发送的是一条**非指向性消息**（`routing.mode = "auto"`），插件自动组装全网候选节点的 `NodeCard`（成员、能力描述 `summary/skills`、当前空闲状态 `load`），调用可配置的判断后端：
     - **`judge-backend-llm-api`**：调用轻量模型 API（如 DeepSeek / Claude Haiku / Gemini Flash）做 JSON 结构化路由选择；
     - **`judge-backend-local-agent`**：直接调用本机注册的 `agy` / `claude -p` / `codex exec` 无头判定器，决定该发给哪个或哪几个节点（支持自动拆分为单播或多播）。

---

### 模块 5：`gate` — 节点权限状态机与入站门禁 (`@the-dealer/gate`)

职责：**守护接收端节点的大门，让节点拥有对入站消息的绝对控制权**。

```typescript
export type AdmissionMode =
  | "open"               // 开放接收：验证签名与允许域后直接放行进入 dispatcher
  | "approval_required"  // 待确认后接收（Knock 敲门模式）：挂起进入本地待审批队列，通知节点主人或主 Agent 确认
  | "dnd"                // 免打扰（僅入队）：接收并持久化到待办队列，暂停自动拉起新终端，待解除 dnd 后派发
  | "closed";            // 完全拒收：直接返回 rejected 回执

export type GateVerdict =
  | { status: "allow" }
  | { status: "pending_approval"; approvalId: string; reason: string }
  | { status: "queued_dnd"; reason: string }
  | { status: "reject"; code: string; reason: string };

export interface GateService {
  getMode(): AdmissionMode;
  setMode(mode: AdmissionMode): Promise<void>;
  /** 细粒度 ACL 策略插件：例如「本机自己的其它节点或 dev 告警流免审 open，跨成员发来的新 task 需要 approval_required」 */
  registerRule(rule: GateRulePlugin): Disposable;
  /** 审批操作：确认放行单条消息（并可选将该发送方/技能加入信任白名单）或拒绝 */
  approve(approvalId: string, opts?: { trustSender?: boolean }): Promise<void>;
  reject(approvalId: string, reason?: string): Promise<void>;
  listPendingApprovals(): readonly PendingApprovalItem[];
}
```
- **极佳的人机体验**：当队友（比如 Oliver）的 Agent 给你的节点发来一条跨模块协作请求，而你处于 `approval_required` 状态时：
  - 发送方立即收到 `{ status: "admitted_pending_approval" }` 回执（知道消息已安全送达你的机器，正在等你点头）；
  - 你的机器弹出一条通知（或在你当前活跃终端显示提示），你只需运行 `dealer gate approve <id>`（或者对当前 Agent 说一句“同意 Oliver 刚发来的请求”），消息立刻进入 `dispatcher` 自动为你拉起 Orca 隔离工作区并启动 Agent 处理！

---

### 模块 6：`dispatcher` — 节点内消息派发器与执行器插件系统 (`@the-dealer/dispatcher`)

职责：**决定通过门禁后的消息在本地如何被排队、合并、限流，并最终交给哪个本地工具/工作区执行**。

我们将你在 `alter-service` 中沉淀的精华设计拆解为 **「调度策略插件 (`PolicyPlugin`)」+「终端执行器插件 (`ExecutorPlugin`)」** 两层正交组合：

```typescript
export type DeliveryStage =
  | "received"           // 网络层已验签接收
  | "pending_approval"   // Gate 挂起等待节点主人确认
  | "queued"             // 已入队（等待 Wave 聚合或等待目标工作区空闲 Idle Gate）
  | "dispatching"        // 正在创建本地资源（如 Orca worktree / terminal）
  | "dispatched"         // 已成功投递给本地 Agent 并获得终端回执
  | "settled"            // 本地 Agent 已完成处理并结案（附带结果摘要或 Reply）
  | "failed";            // 派发异常（保留现场与恢复游标）

export interface DispatchPolicyPlugin {
  name: string;
  /** 对入站消息进行去重、水位检查、波次合并或等待工作区空闲 */
  process(envelope: MessageEnvelope, next: (batch: readonly MessageEnvelope[]) => Promise<DispatchReceipt>): Promise<DispatchReceipt>;
}

export interface ExecutorPlugin<C = unknown> {
  name: string;                  // first-class: inbox, desktop apps, terminal CLIs, orca; legacy aliases: orca-spawn, orca-wave, agent-pty
  /** 检查目标执行环境是否空闲可用（供 Wave Coalescer 的 Idle Gate 调用） */
  checkIdle?(config: C): Promise<{ idle: boolean; reason?: string }>;
  /** 真正执行派发（如创建 Orca 工作区、启动终端、发送 Prompt），返回可持久化的外部资源回执 */
  execute(batch: readonly MessageEnvelope[], context: ExecutionContext, config: C): Promise<ExecutionReceipt>;
}
```

#### 内置服务与插件职责

1. **调度策略插件 (`packages/plugins-policy/`)**：
   - **`plugin-policy-dedup-cas`**：基于 `idempotencyKey` 与单调代次水位（`claim_version / revision`）的本地持久化状态账本，确保同一事件精确派发一次，故障重启后沿用原回执对账，绝不重复建工作区。
2. **持久化信箱、出站 outbox 与唤醒重驱 (`packages/mailbox/`)**：
   - **`MailboxStore`**：唯一原子落盘的信箱账本，承载逻辑 `mailboxId`、单未决租约 `dlv_*`、`wakeTargets` 重驱状态、`inbound:` / `outbound:` 方向化线程路由与旧 `inbox.json` 一次性迁移；通过文件锁 + `fsync`/`rename` 落盘，并以事件唤醒而非轮询驱动消费者。`assertAckableLease` 在发送前只读校验 `--ack` 租约，`pruneOutboundThreadRecord` 只允许删除孤儿 `outbound:` 记录，并按剩余记录自身的信封重算线程状态与 settlement。
   - **`OutboxStore`**：发送端唯一的持久化出站义务账本（`outbox.json` + `outbox-blobs/`）。`claimIdempotency` 在目录锁内按 `(networkId, idempotencyKey)` 原子创建或加入义务，并发调用只能有一个 claim owner；同一条信封幂等入队，记录未确认的目标节点、已确认回执 `deliveredReceipts`、每目标终态失败 `failedReceipts` / `failedNodeIds`、可能已执行但回执超时的 `inDoubtNodeIds`、附件暂存路径、中继已暂存目标、尝试次数与下次退避时间；只有经 emitter 统一校验、属于确认交接阶段的真实回执才标记 `delivered`，签名拒绝/派发失败/本地完整性错误按目标记录并保留暂存证据，只有所有目标都已交付且无失败时才删除暂存文件。普通定时器或另一目标的 scoped 重驱都不能清除未被本次尝试覆盖的 `inDoubtNodeIds`；只有实际发送该目标、真实可达脉冲/`isPeerReachable` 证明在线或显式 force 才会重新判定。`queued` / `delivered` / `failed` 是账本状态，不是进程猜测；仍有可交付目标时条目保持 `queued`。
   - **`OutboxRedriveService`**：outbox 的唯一自动重驱调度 owner，但**不**构造线协议帧、不路由、不解析回执：它通过 `deliver` 回调把条目交给 emitter 的 `redriveOutboxItem`，只持久化返回的确认回执、逐目标失败结论与退避状态。同一 `(networkId, threadId)` 的排队事件按 `createdAt` 因果 FIFO 重驱：队首事件仍在退避、in-doubt 或被 `--id` 排除而未交付时，后续事件不得越过它先发，其他线程不受影响；只有 `--id ... --force` 显式指定单条义务才允许越过同线程队首。`node/peer-updated` 仅对以 `direct: true` 标记的真实可达事件触发；`transport/peer-reachable` 覆盖睡眠唤醒后 `NodeCard.revision` 不变、以及仅通过种子 gossip 或 tunnel 重连的场景（`via: tunnel` 绕过 2s 节流）。两者统一按 `(networkId, nodeId)` 做 2s 节流后只强制该目标，其他条目保持原退避。可达脉冲携带的是 liveness 而非身份，无法解析未知 selector：`transport/peer-reachable` 只处理 `pendingNodeIds` 已包含该 node 的义务（无匹配义务时零 flush、不重写 `outbox.json`）；只有 `node/peer-updated` 以 `includeUnresolved: true` 同时重试 `pendingNodeIds.length === 0` 的未解析义务，让身份目录新增的 NodeCard 真正完成路由解析。另以 2s unref 定时器兜底。总尝试 deadline 在 emitter 内部逐目标递减，outer race 不再丢弃已获得的回执；超时发生在可能已抵达目标的 in-flight 发送时，该目标记入 `inDoubtNodeIds`。普通定时器不会把 in-doubt 目标当普通离线重试；但只要 transport 的 `isPeerReachable` 证明其仍在线（例如慢执行器超过尝试 timeout、或回复包丢失而目标从未离线），下一次常规退避就会用同一稳定 envelope id 重新驱动，并由接收端 `DeliveryLedger` / `dedup-cas` 幂等收敛；`--force` 与真实可达脉冲同样有效。网络卸载不产生终态结果：外部目标保持 `queued` 并保留既有 `inDoubtNodeIds`，重新挂载后继续。失败按 2s → 60s 指数退避，`stop()` 等待当前队列收敛；进程退出后义务仍在磁盘上，由下一次守护进程启动或 CLI `outbox flush` 继续。确定性本地/协议失败的条目只能由操作员显式 `dealer outbox requeue --id <message-id>` 恢复：无签名的失败目标与 `markFailed` 保留的未尝试目标重新进入 pending，尚未解析的选择器保持空 pending 交由 router 重新解析，仍在 in-doubt 的目标继续受 `--force` / 直连脉冲门禁；签名拒绝等终态证据不会被重开或自动重试，暂存证据只在全部目标交付后才删除。
   - **`WakeupRedriveService`**：宿主无关注册表与调度核心，取代易失的 wave-coalescer；以下 Orca 空闲探测、终端操作与轮转由注入的 `packages/plugins-orca` WakeupDriver 唯一实现。目标工作区忙碌或不可判定时只保留持久化 `pendingWake`，绝不注入文本；仅在权威 Orca 工作区/终端空闲门禁通过后投递严格单行指针，空闲探测或投递前准备失败按 1s → 2s → 5s → 10s 退避，安全待投递工作在守护进程重启后恢复。租约确认后达到 `completedWaves` 阈值时，在同一 Orca 工作区新建独立 Agent 会话 Tab 并把 `wakeTarget.driverConfig.terminalHandle` 切流到新 Tab，旧会话 Tab 完整保留且零写入（绝不发送 `/new` 原地重置）；固定工作区默认 `rotateAfterWaves = 1`，每波独占新会话 Tab，首波也不会劫持用户已手动打开的会话 Tab。首次在工作区解析出的 Orca 终端句柄随 `wakeTargets` 持久化，后续波次优先复用同一终端（含已标记 orphaned 但仍 connected 的 TUI），避免固定工作区在多终端之间漂移。
3. **Ring-2 执行器插件 (`packages/plugins-executor/`、`packages/plugins-orca/`、`packages/plugins-agent-host/`)**：
   - **投递结算与未知结果**：`MailboxStore.beginWakeDelivery` 在外部命令或终端注入前持久化 `deliveryInDoubt`，只有成功回执才由 `completeWakeDelivery` 清除此围栏。进程已启动后的错误、abort 或进程中断保留围栏与原因；命令边界确认该次进程从未启动的失败（`CommandNotStartedError`）以及投递前的准备失败（工作区/终端解析、权威空闲门禁、参数校验）不产生围栏，按 1s → 2s → 5s → 10s 指数退避重试。后续入队、强制 flush 与重启均不能自动重放未知结果，须先核实外部结果再显式修复目标；成功结算只覆盖预览序号水位内的消息，新入队消息保留 `enqueue` 决定的截止时间；会话帧即时唤醒，普通波次仍遵守 coalescing。初始执行器（`orca` + mode、命名宿主或兼容别名 `agent-pty` / `orca-spawn`）与后台重驱共用同一套 `deliverAgentHostWakeTarget` / `deliverSpawnedOrcaTerminalWakeup` 投递原语，不各自复制指针构造与终端注入。
   - **Orca spawn 持久交接与启动围栏**：工作区创建并完成信任配置后，交互式任务先写入 `task:<id>` 信箱与 wakeTarget，再创建终端或更新备注；命名宿主执行器也在入队后保持成功交接，唤醒失败不撤回信箱义务。初始与后台终端创建、非任务终端的 shell 启动共用 `launchFencedOrcaWorkspaceTerminal`：命令前持久化 `deliveryInDoubt`，仅 OS 确认未启动或 Orca 结构化 `ok:false` 拒绝可退避重试；中断、畸形回执、缺失句柄及持久化失败保留未知结果围栏，禁止重启后盲目重建。成功句柄与围栏清除原子落盘，随后才等待空闲。任务专属 Tab 在最终投递检查时仍须存活；消失时只通过 `orca terminal create --command` 创建替代 Tab，绝不借用其他 Agent 或裸 shell。单次 `CommandNotStartedError` 不代表整个执行无副作用；只有尚未交接或发生外部修改的执行器边界才能标注 `beforeExternalEffect` 供 dedup 回滚。
   - **宿主会话身份**：`packages/plugins-agent-host` 唯一负责身份映射和启动回执解析。显式 `sessionId` 原样保留；Claude 缺省名称由信箱 ID 可逆编码，Dot/Grok 使用原始信箱 ID。Codex spawn 仅在启动 JSON 的 `threadId`/`sessionId` 可用时启用 fixed 续接，不能用未传给创建过程的显式 ID 冒充新会话身份；无身份的 Codex 与独立 `agy run` 成功后清除自动宿主唤醒绑定，信箱仍可手动读取。已有 fixed 会话不从任务输出重新推断身份。
   - **`executor-orca`（Orca 工作区执行器）**：以 `mode: spawn | wave | fixed` 选择新建子工作区、波次投递或固定会话；负责协作 Request、告警流与常驻工作区投递。
   - **Desktop App 执行器**：`executor-claude-app`、`executor-chatgpt-app`、`executor-antigravity-app`，以同一 `mode` taxonomy 连接桌面会话。
   - **Terminal CLI 执行器**：`executor-claude-code`、`executor-codex-cli`、`executor-antigravity-cli`；独立 `antigravity-cli` 支持 `spawn` / `inbox-only`，其余模式使用 `orca` 或 `antigravity-app`。
   - **`executor-inbox`（持久化人工收件箱）**：`mode: inbox-only` 时只写入 Durable Mailbox，不启动外部进程。
   - **Backward-compatible aliases**：`orca-spawn`、`orca-wave` 与 `agent-pty` 继续解析为对应的一等执行器与会话模式，兼容既有配置。

---

## 四、 配套 Agent Skill 与 CLI 控制面设计 (`dealer-skill`)

项目内置一套标准 `SKILL.md`（可一键安装到 `~/.gemini/skills/the-dealer`、`~/.claude/skills/the-dealer`、`~/.agents/skills/the-dealer`），让任何 Agent 都能通过极为简洁的 `dealer` CLI（全部支持 `--json` 结构化输出）操控全流程：

| 场景分类 | Agent 调用的 CLI 命令示例 | 作用说明 |
| :--- | :--- | :--- |
| **1. 挂载与管理节点** | `dealer node up --name mbp-harlequin --member ben.xu --team sensebeing --json`<br>`dealer node card set --summary "..." --add-skill prod-release:"..." --json` | 启动本机 P2P 守护进程，设置节点身份与能力名片并实时广播到全网 |
| **2. 切换节点权限门禁** | `dealer gate mode approval_required --json`<br>`dealer gate list --json`<br>`dealer gate approve <approval_id> --trust-member oliver --json` | 自由切换 `open / approval_required / dnd / closed`，查看并批准待确认的跨节点消息 |
| **3. 发现全网节点与能力** | `dealer peers list --skill ios-crash-triage --only-available --json` | Agent 发送前可自主查询当前在线的队友节点及其能力描述与空闲状态 |
| **4. 发送消息（定向/多播/广播/AI路由）** | **显式定向**：`dealer send --to member:oliver --kind collab.request --subject "..." --body "..." --attach ./repro.log --json`<br>**AI 智能判断路由**：`dealer send --auto-route --hint "找负责运行时且空闲的节点排查此报错" --body "..." --json`<br>**全网/频道广播**：`dealer broadcast --topic team.sensebeing --subject "..." --body "..." --json` | 一条命令完成跨机器、跨团队成员的实时消息与大附件投递，支持 `--wait-receipt` 或异步流式回调 |
| **5. 配置节点内收发派发器** | `dealer setup apply-preset --host orca --mode spawn --topic collab.request.v4 --agent codex --json`<br>`dealer route add --type local-agent --command codex --json`<br>`dealer source add lark-alert --config ./alerts.yaml --json` | Agent 通过命令直接生成 `patch.yaml`，热挂载新的触发源或本地 Orca/Agent 派发规则 |
| **6. 查看与重驱离线出站** | `dealer outbox list --status queued --json`<br>`dealer outbox flush --id <message-id> --json`<br>`dealer outbox flush --force --json`<br>`dealer outbox requeue --id <message-id> --json`<br>`dealer thread prune <message-id> [--force] --json` | 查看持久化出站义务；对端恢复或目录恢复后立即重发。`queued` 回执表示消息已持久保存，可安全确认本地收件租约，无需轮询等待；`dealer outbox requeue` 仅用于操作员显式重试无签名的本地/协议失败目标。`thread prune` 用于清理发送未承诺时遗留的孤儿 `outbound:` 线程记录：它拒绝 `inbound:` 记录，并在 outbox 仍持有该消息的 `queued`/`delivered` 义务时要求显式 `--force` |

---

## 五、 技术栈与仓库目录结构设计

为了同时达到 **「像 DeepSeek Harness 一样极致的插件可编程性」** 与 **「Rust Iroh 级别的千节点 P2P 高性能」**，建议采用 **TypeScript (Node >= 22, pnpm workspace) + Rust (`iroh` Core Sidecar/Addon)** 混合架构（同时提供 `plugin-executor-script` 与 `plugin-source-script` 让现有的 Python 脚本也能零改造接入）：

```text
the-dealer/                                 # 独立开源仓库根目录（建在 alter-service 相邻目录）
├── Cargo.toml                              # Rust Workspace（编译高性能 Iroh P2P 网络引擎）
├── pnpm-workspace.yaml                     # TypeScript 插件与内核 Workspace
├── package.json
├── README.md                               # 英文主文档（The Dealer 品牌与架构图）
├── README.zh-CN.md                         # 中文完整文档
├── docs/
│   ├── architecture.md                     # 微内核 (Cordis-style) 与六大模块架构设计
│   ├── wire-protocol.md                    # Envelope 签名、QUIC 流、Gossip 与 BlobTicket 规范
│   └── plugin-guide.md                     # 如何用 30 行代码编写 Source / Router / Gate / Executor 插件
├── native/
│   └── dealer-iroh-engine/                 # Rust 实现的 Iroh P2P 核心层 (iroh-net + iroh-gossip + iroh-blobs)
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs                      # 暴露给 TS 内核的高性能 IPC / NAPI 接口
│           ├── endpoint.rs                 # Ed25519 NodeId、mDNS 局域网发现、QUIC NAT 穿透连接池
│           ├── gossip.rs                   # HyParView + PlumTree 节点名片同步与千节点频道广播
│           └── blobs.rs                    # BLAKE3 多媒体与大文件分块流式传输
├── packages/
│   ├── kernel/                             # @the-dealer/kernel：Context、Service 注入、Typed Events、Reversible Effects、Profile/Patch Loader
│   ├── core-types/                         # @the-dealer/core-types：NodeCard、MessageEnvelope、DeliveryReceipt 唯一 Schema 真源
│   ├── process-lock/                       # owner 文件记录格式与进程实例存活判定
│   ├── fs-atomic/                          # 原子写入/复制、append-only 持久日志与共享目录锁的唯一实现
│   ├── transport/                          # @the-dealer/transport：TransportService 接口与 Iroh Engine 桥接插件
│   ├── node/                               # @the-dealer/node：节点身份密钥管理、NodeCard 签名广播、PeerDirectory
│   ├── emitter/                            # @the-dealer/emitter：消息组装、附件自动转 Blob、SourcePlugin 宿主
│   ├── router/                             # @the-dealer/router：三级路由链（Explicit -> Tag Rule -> AI/Agent Judge）
│   ├── gate/                               # @the-dealer/gate：open / approval_required / dnd / closed 状态机与 ACL 规则
│   ├── dispatcher/                         # @the-dealer/dispatcher：入站账本、Policy 链与 Executor 宿主
│   ├── mailbox/                            # @the-dealer/mailbox：持久化信箱账本、租约、方向化线程路由、出站 outbox 重驱与 WakeupRedriveService
│   ├── subprocess/                         # 通用 OS 子进程、PATH 查找与单行文本工具；零内部包依赖
│   ├── plugins-orca/                       # Orca workspace、TUI quirks、wakeup drivers、executors
│   ├── plugins-agent-host/                 # 宿主 descriptors、CLI/App 调用与会话、host driver、named executors
│   ├── plugins-source/                     # 官方内置发送触发源插件包
│   │   ├── source-webhook/                 # 本地 HTTP Webhook 接收器
│   │   ├── source-poll-watermark/          # 通用单水位轮询触发器（可配置对接 Lark / SourceHub / Monitor）
│   │   └── source-script/                  # 挂载任意外部 Python/Shell 脚本作为消息源
│   ├── plugins-router/                     # 官方内置智能路由判断器插件包
│   │   ├── router-llm-api/                 # 调用 OpenAI/Anthropic/DeepSeek兼容 API 根据 NodeCard 判定目标节点
│   │   └── router-local-agent/             # 调用本机 Claude Code / Codex / AGY headless 判定目标节点
│   ├── plugins-policy/                     # 官方内置入站调度策略插件包
│   │   └── policy-dedup-cas/               # 幂等水位与版本 CAS 防重插件
│   ├── plugins-executor/                   # 宿主无关 inbox、notify、raw agent-pty 与副作用工具
│   ├── plugins-lark-notifier/              # Lark 配置真源、验证器与持久化通知
│   └── cli/                                # @the-dealer/cli：`dealer` 与 `dealerd` 命令行入口及内置 Profiles
└── skills/
    └── the-dealer/                         # 配套 Agent Skill（一键安装至 ~/.gemini/skills 或 ~/.claude/skills）
        ├── SKILL.md                        # 供 Agent 阅读的挂载节点、查节点表、发消息、配派发器完整指令
        └── references/
            ├── profile-And-patch-spec.md
            └── writing-custom-plugins.md
```

---

## 六、 分阶段实施路线图（Roadmap）

一旦你确认上述模块契约与目录结构，我们将在相邻目录创建新仓库（如 `/Users/ben.xu/orca/workspaces/the-dealer` 或你指定的平级路径），分 4 个高内聚阶段推进实现：

1. **Phase 1 — 微内核与核心契约底座 (`kernel` + `core-types` + `node` + `gate` + `dispatcher` 状态机)**：
   - 实现类 Cordis 的 `Context / Service / Typed Events (emit, waterfall, bail, serial) / Reversible Effect / Profile & Patch Loader`；
   - 实现 `NodeCard`、`MessageEnvelope`、签名验签、`GateService` 四态门禁状态机与基于 SQLite WAL 的 `DispatcherService` 投递账本。
2. **Phase 2 — Iroh P2P 高性能网络底座 (`native/dealer-iroh-engine` + `transport`)**：
   - 基于 Rust `iroh` 实现 `NodeId` 拨号、mDNS 局域网零配置自发现、跨网 QUIC 直连、`iroh-gossip` 千节点频道广播与 `NodeCard` 实时同步、`iroh-blobs` 多媒体/大文件流式传输。
3. **Phase 3 — 发送路由与接收执行全插件生态 (`emitter` + `router` + `plugins-*`)**：
   - 实现三级智能路由链（显式 ID + Capability Tag + 本地 Agent/LLM Judge 插件）；
   - 实现 `policy-dedup-cas`、持久化 `MailboxStore` 与 `WakeupRedriveService`（波次恢复 + Idle Gate + 单行指针 + 会话轮转）；
   - 实现一等 `executor-orca`、桌面 App、Terminal CLI 与 `executor-inbox`，并保留 `orca-spawn`、`orca-wave`、`agent-pty` 兼容别名。
4. **Phase 4 — CLI、`dealer` 配套 Skill 与 `alter-service` 真实场景迁移验证**：
   - 交付 `dealer` / `dealerd` CLI 与 `skills/the-dealer/SKILL.md`；
   - 在本机实跑双节点/多节点 P2P 自动发现、权限审批（`open` vs `approval_required`）、智能路由判断、大文件传输及自动唤醒 Orca 工作区端到端验收。


### Lark Human Copilot Mirror

`packages/plugins-lark-notifier` exports the first-class Kernel plugin
`lark-mirror` (`createLarkMirrorPlugin`). Gate remains the sole admission owner:
it emits `gate/pending-approval` only when adding a new pending item. The mirror
seeds existing approvals on activation and renders actionable human approval
knocks, observes mailbox arrivals, and mirrors local `emitter/sent` messages into
existing Lark threads. It never approves or dispatches work itself. Kernel effects
own notifier startup/shutdown, including cancellation of retries and commands.

One serialized notifier owns Lark delivery, bounded exponential retries, and
atomic `the-dealer-lark-notify/v1` state. Mailbox catch-up precedes outbound
mirroring, so a watched local loopback is not sent twice. Thread roots and
idempotency keys are shared across approval, mailbox, and outbound delivery;
deleted roots use a distinct fallback key. Pending event queues are in-memory,
not a durable outbound replay log. The local `notify` executor only owns OS
notification or file sinks, with no Lark webhook transport.

`notifyGateApprovals` and `mirrorOutboundThreads` default to true. Default watched
mailboxes omit compound topic/stream bindings and project topics with background
executors whose registry descriptors set `interactiveAgent: true`; explicit `notifyLark` overrides that choice. Inbox topics and top-level request bindings remain included. See
[request threading](request-threading.md) for filters and configuration.

### Automatic upgrades and restart

`dealer self-update` synchronizes a clean main checkout and skill links. `dealer node down` drains active work before shutdown and `dealer node restart` combines that drain with startup; a drain that cannot quiesce leaves the daemon serving and reports `stopped:false`, with `--force` as the explicit override. A shutdown targets the lock-owner incarnation and carries that identity in the control request, so a supervisor-started replacement (for example `launchd` `KeepAlive`) rejects the command and counts as the target having stopped instead of a pending drain. A daemon `runtimeFingerprint` is composed from the code, linked-project manifests/code and user plugins the process actually loaded (mutable config fields never participate), and the daemon's own watcher is the sole automatic reload owner: external changes trigger an idle-gated drain and restart only after active dispatches and control requests become idle. Ordinary CLI commands always forward to the running daemon and never restart it synchronously.


### Stage 3: mailbox core and injected wakeup drivers

| Owner | Responsibilities | Dependencies and limits |
| --- | --- | --- |
| `packages/core-types` | Conversation mode vocabulary and normalization, shared contracts | No runtime state or I/O |
| `packages/mailbox/store.ts` | v2 durable mailbox ledger, mode/stream addressing, opaque `driverConfig`, atomic session pinning and delivered-thread rebinding | Node built-ins, core-types, fs-atomic only; no host interpretation |
| `packages/mailbox/wakeup.ts` | Driver registry, event/timer redrive, pointer construction, claims, backoff and unknown-outcome fences | Generic subprocess error classification only; no host commands |
| `packages/plugins-agent-host` | Declarative host descriptors, invocation/session policy, trust, host driver and host executors | Consumes the mailbox core and generic executor utilities; never imports CLI |
| `packages/plugins-orca` | Workspace and terminal operations, composer quirks, rotation policy, session inference, Orca drivers and executors | Consumes host launch conventions and mailbox primitives; never imports CLI |
| `packages/plugins-executor` | Inbox, notification and raw command/pipe executors, shared preparation/error classification | No host-specific plugin imports; session inference uses the injected redrive service |
| `packages/plugins-lark-notifier` | Lark configuration type/validation and notification delivery | CLI re-exports its configuration; the plugin never imports CLI |
| `packages/cli` | `HostPluginRegistry` assembly root and receiver configuration translation | Registers all wakeup drivers and supplies mode-only mailbox addressing |

`the-dealer-mailbox/v1` files load without rewriting. The next successful mutation atomically
writes v2, preserving driver-specific fields inside `driverConfig`, existing leases, threads,
rotation counters, and unknown-outcome fences. The only host identifier in the mailbox core is
the explicitly approved one-line legacy migration from configured passive targets to the
`agent-host` driver; normal v2 operation treats all driver ids as opaque strings. Unknown v2
wake-target keys and non-JSON configuration values are rejected.

A driver persists `beginWakeDelivery` before an external boundary and settles only observed
sequences. Confirmed delivery may atomically pin its session mailbox and delivered thread routes;
mail arriving during delivery remains pending. Proven command non-starts back off, while unknown
outcomes remain fenced across mail arrivals, forced flushes and restarts. Orca drivers retain
idle proof and empty target-composer verification, create independent tabs for rotation, and never reset a
retained session. Any human draft or unconfirmed Turn Pilot recommendation holds mail until the
composer is empty; wakeups never clear or submit that content. Unregistered drivers retain pending obligations with an explicit retry error.
