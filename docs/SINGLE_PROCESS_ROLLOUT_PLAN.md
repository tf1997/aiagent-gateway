# 整个网关灰度发布实施方案

更新日期：2026-10-19。状态：实现中，Linux/systemd 原型和单进程基础运行时已验证；完整自动灰度尚未完成。具体交付、验证边界与下一步见 [实现进度](SINGLE_PROCESS_IMPLEMENTATION_STATUS.md)。

本文基于当前 host + worker 代码，定义入口、业务、状态和发布控制一起换版的方案。当前安装仍为 2 → 3 → 2，普通 install 只灰度 worker，host 更新需要维护窗口；实际操作以 [部署全流程](DEPLOYMENT_LIFECYCLE.md) 为准。本文中的新接口、字段、命令和默认值均为目标设计，不能直接用于当前版本。

## 1. 目标与范围

采用一个完整网关常驻、发布时运行新旧两个完整网关的结构：

~~~text
稳定：       A〔入口 + 业务 + 状态 + 发布控制〕
灰度：       A〔当前 owner〕 ← 私有 UDS → B〔候选〕
交接完成：   B〔入口 + 业务 + 状态 + 发布控制〕

服务进程：   1 → 2 → 1
连续升级：   A → B → C，每轮最多两个服务进程
~~~

固定以下决策：

- 一个非 root 的 systemd service，不增加常驻 supervisor、反向代理、状态服务或数据库。
- 继承监听 FD 交接公共端口；统一分配器负责逻辑会话灰度。
- 灰度期 A 是唯一状态写入者，B 通过私有通道访问 A；最终在线交接写入权。
- 保留真实业务样本、会话与 Agent 亲和、失败回退、长流排空、CLI 断开后继续发布。
- 尚未正式上线，直接建设唯一新拓扑。移除部署中的 host/worker 角色分支和 infrastructure 字段，不增加 deployment.json.version，不实现旧部署兼容或自动拓扑迁移。
- 第一版热升级要求相邻产物能理解同一套状态存储、快照和私有协议，不支持在热升级中执行不可逆存储格式迁移。握手拒绝不兼容产物，不等于维护历史部署模式。

进程数只统计提供服务的网关实例，用户启动的 install/status CLI 不计入。排空超时可以保留两个实例，禁止启动第三个实例。

### 1.1 各组件如何验证

| 组件 | 验证方式 | 正式接管时机 |
| --- | --- | --- |
| 业务处理、HTTP/SSE/Hermes/A2A 协议 | 5% 新逻辑会话，再进行全量观察 | 全量阶段处理后续请求 |
| 公共 HTTP 接入、连接管理 | 候选私有预检；全量阶段采集新版直接接入的真实样本 | 全量阶段接入新连接 |
| 状态读写实现 | 快照导入、增量重放、摘要及不变量检查；提交后的运行核查 | 状态交接提交点 |
| 发布控制、进程管理 | 状态机恢复测试、MainPID 实测、连续 A→B→C | 状态提交后接管控制 |

5% 指新逻辑会话的分配比例，不是连接比例，也不保证请求数恰好为 5%。新版入口在第一版中没有独立的 5% 公网连接灰度；单写者状态也不会由两版按 95%/5% 同时写入。整包换版的覆盖边界必须如实展示。

## 2. 进程内结构与代码边界

~~~text
GatewayProcess
  PublicServers       数据面 :8080、管理面 :9090、连接排空
  TrafficRouter       统一版本分配、会话亲和、流量 revision
  BusinessRuntime     当前 worker 的业务 handler 和结果采集
  StateAccess         状态调用统一入口，可本地执行或 UDS 调用
  StateOwner          配置、会话、资源、租约、幂等、配额、回执
  ReleaseController   发布推进、样本判断、恢复与交接决策
  ProcessManager      候选启动、身份验证、监听 FD、systemd 生命周期
~~~

这些都是同一 PID 内的模块。稳定模式下，入口直接调用本地业务 handler，业务进入本地状态 dispatcher，不经过本机 TCP/UDS。

| 调用 | A | B |
| --- | --- | --- |
| 灰度分配、状态访问 | 本地唯一 dispatcher | UDS 调用 A |
| 执行目标属于自身 | 本地 handler | 本地 handler |
| 执行目标属于对方 | 私有 UDS，最多转发一次 | 私有 UDS，最多转发一次 |
| 状态提交后 | 只允许排空与必要的控制转发 | 本地 dispatcher |

拟新增 `internal/process` 组装完整进程，`internal/handoff` 实现交接协调。复用 `internal/entry`、`internal/rollout`、`internal/state`，避免重写现有业务协议。

StateAccess/交接协调器需要以下能力，具体 Go 类型在 P1/P2 实现时确定：

~~~text
Call(ctx, operation) -> result
Prepare(handoff) -> snapshot manifest
Follow(snapshot revision) -> ordered deltas
Seal(handoff) -> final revision + digest
Adopt(handoff, expected digest) -> committed owner
Abort(handoff) -> confirmed original owner
~~~

本地与远程调用必须经过相同的认证上下文、幂等、配额、持久化和结果采集逻辑。不能仅把当前 Remote 指针改成可空：现有 worker 依赖 Remote，状态客户端遇到 epoch 变化会永久失败，租约还保存 Go 回调，均须先重构。

## 3. 启动、监听与请求路径

### 3.1 候选启动与握手

1. root CLI 把新二进制暂存到 `/opt/aiagent-gateway/releases/<sha256>/gateway`，核对所有权、权限、真实路径与哈希，再通过本机控制 UDS 提交发布。
2. A 持久化发布意图，生成 rollout_id、handoff_id、B 的 incarnation 和一次性握手凭据，启动 B。
3. B 只打开自己的私有通信端点，恢复只读配置视图，不打开业务数据写句柄、不启动后台核查、不接收公共连接。
4. 双方核对产物、incarnation、配置 revision、私有协议能力、快照格式和业务 profiles；不能只相信候选自报 PID。
5. B 的私有 readiness 通过后，才允许进入 5% 验证。

私有 socket 使用 `/run/aiagent-gateway/p/<短随机标识>/peer.sock`，控制路径长度。完整身份放在握手消息里，不能依靠短路径或 PID 判定版本。

候选 FD 采用明确白名单，拟定如下：

| FD | 内容 | B 何时使用 |
| --- | --- | --- |
| 3 | 数据面 listener 副本 | 全量接入阶段 |
| 4 | 管理面 listener 副本 | 全量接入阶段 |
| 5 | 稳定控制 UDS listener 副本 | 状态提交后 |
| 6 | 只读配置文件 | 启动解析 |
| 7 | 启动握手与父进程存活通道 | 启动至生命周期交接 |

通过 Go `exec.Cmd.ExtraFiles` 和 `net.FileListener` 实现。B 暂不调用公共 listener 的 Accept；所有未列出的 FD 都关闭，特别是数据锁、journal、已有业务连接和无关凭据。[Go FD 继承](https://pkg.go.dev/os/exec#Cmd)、[Go FileListener](https://pkg.go.dev/net#FileListener)

### 3.2 5% 请求分配

~~~text
客户端 → A 公共入口 → A 统一分配器
                         ├─ 原有会话 / 未命中灰度 → A 业务 → Agent
                         └─ 新会话命中 5%       → B 业务 → 原 Agent
                                                   ↓ 状态调用
                                                A StateOwner
~~~

- 沿用当前逻辑会话 key 与首次分配规则，由 A 串行确认版本归属；两版不能各维护一套随机数或计数器。
- 灰度阶段已有会话保持网关版本亲和；Agent Endpoint 归属始终保持，改变网关版本不能重新选择 Agent。
- 公共入口清理外部伪造的内部头，生成 request_id，并附上 rollout_id、routing_revision、目标 incarnation 和私有凭据。
- 私有执行端点不再进行版本分配，避免 A→B→A 循环；目标身份或 revision 不匹配时，在访问上游前拒绝。
- 同一请求只执行一次。请求已发送到候选或 Agent 后发生故障，不能换版本重放 POST、模型调用或工具操作。
- 本地调用和 UDS 转发统一产出结果；分别记录 ingress_incarnation 与 execution_incarnation，相同 request_id 去重。

### 3.3 全量入口交接与排空

验证通过后，A 持久化流量意图并把后续业务请求全部分配给 B，然后启用 B 的公共 Accept，确认接入可用后停止 A 的公共 Accept。A 保留可重新构造 listener 的 FD，以支持提交前回退。

共享的是内核监听 socket，已有 TCP/SSE 连接仍属于原进程。旧连接上的后续请求按全量策略交给 B，已经开始的请求保持原执行链。旧入口关闭空闲 keep-alive、在适当的 HTTP/1.1 响应上要求关闭连接，并等待活动请求结束。[Envoy 热重启对监听与存量连接的说明](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/operations/hot_restart)

进入全量观察后，除业务成功样本外，要求每个必需 profile 至少取得 10 个 `ingress=B && execution=B` 的新成功样本；同一样本可以同时满足这两项门槛。所有请求一直复用 A 的旧连接时，不得仅凭转发成功提交。

管理面同时交接接入，但配置修改在发布期间保持冻结。只读管理访问由当前 StateOwner 提供一致结果；发布控制使用受限本机 UDS。

必须分别计数：公共活动请求、业务执行、A→B/B→A 转发、状态调用、后台任务和持有者租约。A 的本地业务数归零不代表可以退出，A 转发给 B 的长流也必须结束。

第一版按当前 HTTP/1.1 能力验收；启用 HTTP/2 前另外实现和验证 GOAWAY、stream 排空。listener 继承不会迁移已有连接。

## 4. 发布状态机与门槛

~~~text
STABLE
  → PREPARING → STARTING → VERIFYING
  → PROMOTING → OBSERVING → DRAINING_OLD
  → HANDOFF_PREPARED → HANDOFF_SEALED
  → COMMITTED → ACTIVATING → COMPLETED

提交前验证失败：
  → ROLLING_BACK → DRAINING_CANDIDATE → ROLLED_BACK

无法安全推进或回退：
  → RECOVERY_REQUIRED，保留 handoff_id、最后确认阶段和阻塞原因
~~~

| 阶段 | 推进条件 | 本阶段失败处理 |
| --- | --- | --- |
| PREPARING / STARTING | 产物与协议校验；B 私有 readiness 通过 | 退出 B，A 继续 |
| VERIFYING | 5% 新会话；最短时间和每 profile 成功门槛满足；无有效失败 | 切回 A，排空 B |
| PROMOTING / OBSERVING | 全量后续请求；B 公共入口与业务样本分别达标 | 恢复 A 接入与执行，排空 B |
| DRAINING_OLD | A 的公共请求、转发、业务执行及不可迁移任务均归零 | 超时保留两个进程，等待 resume 或 rollback |
| HANDOFF_PREPARED / SEALED | 快照追平；状态调用屏障完成；导入和摘要验证通过 | 根据持久决定与实际写锁核对能否恢复 A |
| COMMITTED / ACTIVATING | B 获得写入权；冷启动选择 B；控制及 MainPID 交接确认 | 恢复 B，不再自动回到 A |
| COMPLETED | B 独立可用、A 已退出、MainPID 正确、后台 owner 唯一、配置解冻 | 正常运行故障恢复 |

沿用现有门槛：readiness 30 秒；验证至少 60 秒、最长 300 秒，每个必需 profile 至少 10 个成功；有效失败阈值 0；全量观察至少 300 秒并取得新成功样本；排空期限 600 秒。profiles 由配置推导，覆盖 HTTP、SSE、A2A、A2A SSE。

全量观察满 300 秒仍缺样本时进入可恢复的等待状态，不能当作成功。健康检查、Agent Card、未访问 Agent 的响应不计业务成功；SSE 首帧成功后中断、HTTP 200 内的协议错误、未知结果均不能补成成功证据。

普通阶段转换继续使用“先落盘意图、再应用、读取实际 revision 核对”的方式，响应丢失后依据事实恢复，不重复创建候选。状态提交的额外规则见下文。

## 5. 可交接状态模型

### 5.1 单写者与锁

新增 `data_dir/owner.lock`，覆盖整个进程的状态所有权。唯一 owner 才能持有配置、shared、entry、release 等领域的写句柄；现有领域锁继续保留，不能仅交接 shared 的锁。

锁文件 inode 固定，不通过删除或替换锁文件“解锁”。所有 writer 先取得 owner.lock，再按固定顺序打开领域锁；释放时先关闭领域写句柄，最后释放 owner.lock。候选不能继承这些 FD。

A 让出锁后即被禁止写入，不能因为 B 响应超时自行恢复。锁与 epoch 一起约束所有写路径，包括后台核查、配置持久化、发布报告和结果回执。

### 5.2 导出内容

| 状态域 | 必须保留的信息 |
| --- | --- |
| 配置 | revision、消费者、Token、route/Endpoint ID、恢复事务、管理幂等 |
| 会话 | caller/route/interface/session 到 Endpoint 的绑定；网关灰度归属 |
| Hermes/A2A 资源 | 公开 ID、别名、父子关系、创建预留、生命周期、未确认执行 |
| 并发与引用 | 全局/调用方/Endpoint 占用、资源引用、活动租约、执行持有者 |
| 请求与幂等 | operation_id、request_id、指纹、已提交结果、未确认状态、灰度样本去重 |
| 临时一致性状态 | 未过期分页快照、游标、缓存的原有效期及容量占用 |
| 后台任务 | 可恢复任务描述、调度归属、未确认上游工作；不迁移 goroutine |
| 发布控制 | 当前报告、流量 revision、待完成意图、交接记录、冷启动选择 |

内存中的状态也在交接范围内。当前 Leases 保存 release 闭包，改为显式记录：

~~~json
{
  "lease_id": "lease_...",
  "holder_incarnation": "B_...",
  "caller": "consumer_...",
  "route": "route_...",
  "endpoint": "endpoint_...",
  "resource_refs": ["resource_..."],
  "quota_debits": {"global": 1, "caller": 1, "endpoint": 1},
  "kind": "request"
}
~~~

release 根据记录执行一次性释放，迟到或重复释放不能再次扣减。B 的在途请求保持原 lease_id；切换 owner 不等于所有执行者死亡。崩溃恢复则核对持有者身份并清理已确认死亡的执行占用，资源的未知上游结果不能一并清零。

### 5.3 快照、增量与状态调用屏障

引入独立 logical_revision，覆盖所有可交接状态变化，包括内存租约、回执和游标。快照与增量携带 `owner_epoch + logical_revision`；同一 epoch 内严格递增，日志压缩不能重置它，重启后换 epoch。现有 Journal.sequence 在压缩时会重置，不能直接当作复制游标。

StateAccess 的接纳门覆盖完整状态操作，包括锁外 discovery、持久化和回执登记。操作进入时绑定 epoch 和 operation_id，提交前再次校验；不能只等 shared.Server.mu 空闲就认为没有在途写入。

快照在一致 revision 上建立，只读导入不产生上游副作用。A 在提交状态变化的同一临界区记录有序增量，B 只更新隔离的导入视图。增量溢出或 revision 缺口必须触发重新取快照或停止本轮准备，禁止跳过变化继续提交。

快照按不超过 1 MiB 的块传输，每块包含 snapshot_id、序号、偏移、长度和摘要；B 验证顺序、总量及最终摘要后才承认导入完成，不能把整个快照塞进当前状态 RPC。摘要基于明确排序与编码的内容计算，不依赖 map 遍历顺序。交接日志引用快照摘要，快照不反向包含这条引用，避免循环依赖。

导入校验至少覆盖：资源引用可解析、会话 Endpoint 存在、幂等键与指纹唯一、租约和后台执行能与配额计数对账、游标有效期未被延长。定时清理产生的状态变化同样进入增量流；导入 B 在提交前不独立推进过期清理。

后台调度器仅在当前 owner 启用。建立最终屏障时停止调度并等待/取消受控状态调用；不能持全局锁等待任意长的上游网络请求，也不能让旧 goroutine 在封存后继续落盘。

## 6. 交接协议与持久提交点

### 6.1 唯一权威记录

新增独立的 `data_dir/handoff/` 元数据日志，只由持有 owner.lock 的进程写入，采用带校验和、完整记录边界和 fsync 的提交方式。固定提交信封能由冷启动入口读取，不需要启动旧版业务 runtime 来选择版本。

每次交接至少保存：

~~~json
{
  "handoff_id": "handoff_...",
  "rollout_id": "rollout_...",
  "phase": "SEALED",
  "source": {"incarnation": "A_...", "artifact_sha256": "..."},
  "target": {"incarnation": "B_...", "artifact_sha256": "..."},
  "owner_epoch": "epoch_A",
  "next_owner_epoch": "epoch_B",
  "config_revision": 42,
  "logical_revision": 1086,
  "snapshot_format": "gateway-state-1",
  "snapshot_digest": "...",
  "cold_start_artifact_sha256": "A 的 sha256"
}
~~~

这是协议示意，不是 deployment.json 的新版本字段。快照 manifest 还列出各状态域的 revision、摘要、条数及字节数。

唯一提交点是 B 持有全部写锁后，持久化一条 COMMITTED 记录，同时确定 `owner=B / epoch=新 epoch / cold_start=B / snapshot_digest`。状态归属和冷启动选择不能依赖两个分别更新的文件。普通 release 报告和可选 current 缓存均为投影，崩溃恢复以此决定为准。

第一版提交前不修改正式存储格式、不让 B 开始业务写入。只读导入和临时快照可以失败并清理；失败不得污染 A 正在使用的数据。

### 6.2 正常交接顺序

1. **准备**：全量观察通过，A 已排空公共请求、业务执行和跨进程转发。A 持久化 PREPARED，B 导入一致快照并追赶增量；B 的业务仍访问 A 的状态。
2. **屏障**：暂停新的状态调用与后台调度，等待已接纳的操作完成。B 新状态调用进入有界队列；已经传输的网络流不因此关闭。
3. **封存**：A 刷盘各领域和最终快照，记录活动 B 租约、未确认回执及最终 revision。先持久化快照文件和目录，再持久化 SEALED；B 验证完整导入及全局不变量。
4. **让锁**：A 停止所有写入，关闭领域 journal 与 handoff 写句柄，最后释放 owner.lock；通过握手通道通知指定 B 接管。A 不再推进普通控制器 Tick。
5. **提交**：B 取得 owner.lock 和各领域锁，重读 SEALED 并核对身份与摘要，再持久化 COMMITTED。任何写入或 fsync 结果不明确都进入恢复核对，不能假定未提交。
6. **启用**：B 将 StateAccess 切成本地，恢复队列与单一后台调度器，接管分配器和发布控制。A 仅保留生命周期收尾与必要的控制转发。
7. **进程身份交接**：B 解除提交前的父进程退出联动，完成控制 listener、systemd MainPID 和启动记录的实际核对；A 收到确认后退出。
8. **完成**：B 确认 A 已退出、所有权与 readiness 正常，解冻管理配置，持久化 COMPLETED；后续 B 可以独立启动 C。

COMMITTED 以后不再自动回到 A，即使 MainPID 尚未切换或 A 没收到回执。ACTIVATING 失败报告为“已提交、接管待恢复”，不能显示回滚成功。

### 6.3 跨 epoch 的幂等与迟到操作

当前客户端把 epoch 变化视为永久故障，需要增加只针对已验证 handoff 的显式 Adopt；遇到任意新 epoch 不能自动信任。

- 每个状态操作在首次调用前分配 operation_id，重试及跨 owner 重试保持不变。
- 指纹包含业务动作、持有者和参数，排除传输 epoch、重试次数等字段。当前对整个 Request 哈希会让跨 epoch 重试发生假冲突，必须调整。
- 迁移现有回执缓存、未确认操作和淘汰边界。重放业务结果不变，响应信封使用当前已认证 owner；不能返回旧 epoch 导致客户端再次失效。
- 操作已提交但回执丢失时返回原回执；找不到回执则核对持久幂等记录，无法确认时返回结果未知，禁止盲目执行。
- 迟到 release 根据原 lease_id 和 holder_incarnation 释放一次，拒绝其他持有者伪造释放。
- 业务请求已经到达 Agent 后，状态重试失败也不能触发业务重放。

### 6.4 初始时间与容量预算

以下为原型起始默认值，须在 P2/P5 实测调整。作为 Policy 显式配置传入并写入报告，不隐藏在环境变量中。

| 参数 | 起始值 | 达到限制时 |
| --- | --- | --- |
| 快照准备与追赶期限 | 120 秒 | 停止本轮准备，保留 A owner |
| 最终屏障预算 | 5 秒 | 未让锁可取消；已让锁先核对提交结果 |
| 单状态调用排队时间 | 最多 2 秒且不超过原 deadline | 按操作是否提交返回明确不可用或结果未知 |
| 状态等待队列 | 1024 项、16 MiB，两者先到为准 | 入队前拒绝；不缓存业务请求正文 |
| 单轮快照总量 | 256 MiB | 准备阶段拒绝，不能截断导入 |
| 增量缓存 | 32 MiB | 标记追赶失败，重新准备或等待恢复 |
| MainPID/控制交接核对 | 30 秒 | 保留两个进程，报告 ACTIVATING 待恢复 |

状态调用不扩大原 deadline。队列容量包括请求和待返回结果的实际占用；进行中的流只在需要状态操作时等待。以上预算不是零失败或固定毫秒切换承诺。


## 7. 回退与崩溃恢复

### 7.1 提交前回退

A 仍持有写锁时，恢复 A 的公共接入并把后续业务请求切回 A，停止 B 接入，再排空 B。已经开始的 B 请求继续使用 A 的最新状态；B 的业务、转发和租约确认结束后才能退出。不能恢复灰度开始前的状态快照。

若 A 已让锁但还未确认提交结果，rollback 必须走协调协议：

1. 先要求 B 停止接管；只有取得 owner.lock 的协调方才能作出新决定。
2. 重新读取交接日志。若存在 COMMITTED，立即按 B 恢复，拒绝回退 A。
3. 若尚未提交，A 取得 owner.lock 并持久化 ABORTED，才允许恢复写入；B 后续拿到锁也必须先检查 ABORTED，不能继续提交。
4. B 仍持锁、无法确认退出或存储状态不明时保持 RECOVERY_REQUIRED。超时不等于获得接管授权，禁止两个进程抢着写。

ABORTED 保留当前最新业务状态，只撤销该交接授权。所有权恢复成功后再恢复流量；无法安全恢复时不伪造“回滚完成”。

### 7.2 故障矩阵

| 故障点 | 权威版本/决定 | 处理 |
| --- | --- | --- |
| B 启动或协议检查失败 | A | 清理 B，A 继续服务 |
| B 在验证/观察阶段崩溃 | A | 后续流量切回 A；已发送请求不重放；确认 B 死亡后处理其租约 |
| A 的旧 SSE/转发一直不结束 | A | 排空超时保留两个进程，resume 等待或提交前 rollback |
| 快照缺块、增量断档、摘要/容量校验失败 | A | 拒绝交接，重新准备或回退 |
| A 已封存、尚未让锁 | A | 明确取消屏障后恢复 A；不使用过期导入结果 |
| A 让锁后 B 尚未提交 | 最近已提交版本 A | 按锁和交接记录确认 ABORT；不能仅靠 RPC 超时恢复 A |
| B 写 COMMITTED 时返回错误/进程退出 | 需核对有效记录 | 获得锁后读取决定；完整有效 COMMITTED 选择 B，否则按未提交恢复；不猜测 fsync 结果 |
| B 已提交，回执丢失 | B | 恢复 B；A 禁止再写 |
| B 已提交，MainPID 交接失败 | B | 保留现场并重试接管；故障重启也选择 B |
| A 在提交前崩溃 | A | 第一版由 systemd 清理同组残留，再冷启动 A；在途连接可能中断 |
| A 在提交后崩溃，MainPID 仍为 A | B | systemd 若清理整个服务，随后依据提交记录启动 B |
| B 成为 MainPID 后崩溃 | B | systemd 重启 B |
| 两版/机器重启 | 最近持久提交决定 | 未提交当前交接选 A；已提交选 B；不恢复丢失的成功样本 |
| 日志损坏、摘要不符、产物缺失 | 无法确定可用实例 | 停止业务接纳并报告原因，不选另一个版本碰碰运气 |
| CLI 退出或重复 install | 原发布任务 | 控制器继续；同产物返回原任务，不启动第二个候选 |

第一版不做“旧主进程突然死亡，候选无损接管所有旧连接”。计划升级的平滑交接和单机故障恢复分别验收。

## 8. systemd、冷启动与控制通道

### 8.1 service 目标配置

保留现有 User/Group、LoadCredential、目录权限和沙箱限制；主要调整如下。其余配置仍沿用当前 unit，这段不是完整可安装文件。

~~~ini
[Service]
Type=notify
NotifyAccess=main
ExecStart=/usr/local/bin/aiagent-gateway launch --config=%d/config.yaml --deployment=%d/deployment.json
Restart=on-failure
KillMode=mixed
TimeoutStartSec=60
TimeoutStopSec=35
~~~

首次启动由完整进程在状态恢复和公共接入就绪后发送 READY=1，移除当前 unit 的 ExecStartPost 就绪探测，避免两套就绪条件各自判定。

launch 是拟新增的短命启动入口：读取提交记录、验证不可变产物，然后 exec 为选中的完整网关，PID 不变，不留下一个常驻父进程。候选使用同一个完整运行入口和额外握手 FD，转正后具备独立发布能力。

人工 stop/restart 使用有界停机：当前主进程负责让两版停止接纳并排空；超过 TimeoutStopSec 时由 systemd 清理残留。普通灰度不调用 systemctl restart，也不复用人工停机的强制终止路径。[systemd 终止语义](https://github.com/systemd/systemd/blob/v247/man/systemd.kill.xml)

### 8.2 MainPID 交接次序

1. B 确认 COMMITTED 已持久化，解除“父进程退出则候选退出”的提交前保护。
2. A 以当前 MainPID 身份发送 MAINPID=B；发送成功只代表消息发出。
3. B 核对 systemd 实际 MainPID，成为被接受的通知方后发送 READY=1 和同步屏障。
4. 双方核对实际 MainPID 与服务状态，B 通过私有通道回复已接管。
5. A 退出，B 确认旧 PID 消失，再标记 COMPLETED。

NotifyAccess=main 下不能假定 A 被替换后还能发送有效通知。具体采用 sd_notify 通知与屏障的实现必须通过 P0 的真实 Linux/systemd 测试。[systemd MAINPID 与 BARRIER 协议](https://github.com/systemd/systemd/blob/v247/man/sd_notify.xml)

当前 ChildProcessDriver 的父管道关闭即结束子进程逻辑仅用于提交前候选。提交后必须按交接决定改变生命周期；不能让 A 正常退出顺带杀死 B，也不能取消所有提交前保护导致孤儿候选继续服务。

### 8.3 冷启动版本选择

| 持久记录 | launch 选择 |
| --- | --- |
| 从未建立过发布基线 | deployment.initial_release；由首次启动持久化初始基线 |
| 已有基线，没有待处理交接 | 最近提交产物 |
| 本轮只有 PREPARED / SEALED / ABORTED | 上一已提交产物 A |
| 本轮已 COMMITTED，普通报告还未完成 | B |
| 元数据存在但不可解析、产物无法校验 | 失败并报告；不能回退 initial_release |

启动的完整进程必须先取得 owner.lock，再核对提交决定与自身产物一致，之后才允许打开业务写句柄或公共接入。若启动选择期间决定变化，应重新选择并 exec，不能拿到锁就继续运行错误版本。

恢复时生成新 incarnation/epoch，保留最新业务状态，核对已死亡持有者的租约和未知上游结果。提交日志解析明确区分完整有效记录、可恢复的末尾未完整记录和中间损坏；恢复不得跳过损坏记录伪造提交链。

root 管理 CLI 和短命 launch 入口不承载网关服务。第一版固定启动信封协议，使旧 launch 可以选择新产物；CLI/安装器自身更新由 root 安装流程负责，不要求运行账户覆盖 root 二进制。

### 8.4 控制与内部端点

稳定控制路径保留 `/run/aiagent-gateway-control/controller.sock`。通过继承 listener 交接，避免 unlink/rebind 的空窗；UnixListener 明确关闭“Close 时自动 unlink”，旧进程不得删掉新版仍在使用的路径。私有端点只由自己的 incarnation 清理。

状态提交前由 A 接受控制连接；提交后 B 开始 Accept，A 停止 Accept，已有控制连接短暂转到 B 或返回可重连状态。CLI 使用原 rollout_id 重连，不能把断连当成任务不存在。屏障期间 status 可返回带“交接中”标记的最后确认视图，修改类控制必须经协调器串行确认。

发布/执行权限保留原边界：root 暂存只读产物，通过 Unix peer credentials 授权执行；公共管理 Token 不能指定任意二进制。内部凭据经专用通道传递，日志与 status 不输出 Token 或凭据。

接纳状态拆为 ingress_accepting、execution_accepting、state_accepting、storage_healthy、controller_owner。旧版停止公网接纳后仍需提供状态服务，不能直接调用现有 BeginShutdown 把这些状态一并关闭。

## 9. 安装与操作接口

### 9.1 部署文件

目标 deployment.json 只保留首次启动产物与发布策略：

~~~json
{
  "initial_release": "/opt/aiagent-gateway/releases/<sha256>/gateway",
  "policy": {
    "auto_profiles": true,
    "canary_percent": 5,
    "readiness_timeout_seconds": 30,
    "verify_min_seconds": 60,
    "verify_timeout_seconds": 300,
    "min_success_per_profile": 10,
    "max_effective_failures": 0,
    "required_profiles": ["http"],
    "observe_seconds": 300,
    "observe_min_success_per_profile": 10,
    "drain_timeout_seconds": 600
  }
}
~~~

新增入口样本门槛和第 6.4 节交接预算由 Policy 解码器赋明确默认值，也可显式覆盖；status 输出最终生效值。initial_release 只用于没有发布基线的首次启动，普通升级不反复修改它。

拟新增路径：

| 路径 | 内容 |
| --- | --- |
| data_dir/owner.lock | 完整网关的唯一写入权 |
| data_dir/handoff/ | 交接决定、最近提交产物和恢复元数据 |
| data_dir/handoff/snapshots/ | 完整且有摘要的交接快照与临时导入文件 |
| /run/aiagent-gateway/p/<短标识>/ | 每个进程的私有通信与身份端点 |

已有配置文件和 shared/entry/release 领域数据由完整进程统一管理。旧产物与快照清理必须确认没有活动发布、待恢复交接或当前启动记录引用，不能在 A 刚让锁时删除恢复依据。

### 9.2 命令的目标行为

~~~sh
sudo ./gateway-new install
sudo aiagent-gateway rollout status --json
sudo aiagent-gateway rollout rollback
sudo aiagent-gateway rollout resume
sudo aiagent-gateway status
~~~

- 首装启动一个完整实例；后续 install 发布完整二进制，不再区分更新 worker 与更新 host，目标模式移除 --update-host。
- root CLI 只负责暂存并提交任务，后续放量和交接由运行中的控制器推进；CLI 中断不撤销发布。
- 重复提交相同产物复用原任务；存在未退出旧实例或未解决交接时拒绝另一产物。
- rollback 在提交前恢复 A；提交后返回明确的 ROLLBACK_AFTER_COMMIT，status 指示以兼容当前状态的产物发起新发布。
- resume 根据持久阶段、身份、锁和实际流量 revision 核对后继续，不把计时器超时当成可跳过的步骤。

现有本地测试安装切换到新拓扑时，停止并核对旧进程退出，人工备份配置与业务数据，再按新拓扑部署。不建设历史安装兼容分支，不自动删除或重置用户数据；新拓扑内部的 A→B 在线交接是本文实现范围。

### 9.3 status 与运行证据

至少输出以下信息，并保留到终态报告：

- rollout_id、handoff_id、phase、last_confirmed_phase、committed、阻塞原因和可执行操作。
- A/B 产物摘要、PID、incarnation；systemd 实际 MainPID。
- state_owner、epoch、logical_revision、config_revision、cold_start_artifact。
- 当前流量 revision、灰度比例、每 profile 的业务样本和新版直接入口样本。
- 两版公共连接/活动请求/业务执行/转发数量；状态调用、租约和后台任务数量。
- 快照总量、增量滞后、屏障耗时、等待队列长度/字节/超时数量。
- 各阶段时间、最后一次存储错误和身份交接确认时间。

COMPLETED 必须同时满足：B 独立可用、状态和控制归属为 B、systemd 跟踪 B、冷启动选择 B、A 已退出、配置解冻。不能仅根据新业务样本达标报告成功。

## 10. 代码改造清单

| 位置 | 具体工作 |
| --- | --- |
| cmd/gateway/main.go、roles.go | 增加 launch/完整运行入口、候选握手和 FD 校验；删除 host/worker 部署角色 |
| internal/host/host.go → internal/process/ | 整合业务执行；拆分接纳、状态与控制生命周期 |
| internal/service/child.go、worker.go | 改成完整候选管理；FD 传递、身份、提交前父子保护、提交后独立运行 |
| internal/service/rollout.go、host_install.go、units/ | 单拓扑安装、启动选择、Type=notify、移除 infrastructure 与 --update-host |
| internal/entry/entry.go | 本地/私有两种目标、唯一分配器、双入口结果、恢复 Accept、连接/转发排空 |
| internal/rollout/controller.go | 新阶段与提交边界；恢复逻辑以 handoff 决定为准；不提前停止状态 owner |
| internal/releaseprotocol/protocol.go | 握手能力、handoff 身份、入口/执行样本、接纳状态、有效 Policy 与 status |
| internal/gateway/runtime/rollout/worker.go | 消除必须 Remote 的限制；本地/远程统一结果判据与状态调用 |
| internal/gateway/runtime/shared/ | 可序列化租约、语义指纹、回执/淘汰边界迁移、显式 Adopt |
| internal/gateway/runtime/ 相关配置/资源/会话模块 | 统一 StateAccess、完整快照、后台归属和活动引用恢复 |
| internal/state/journal.go + 新 internal/handoff/ | owner 锁、只读导入、逻辑 revision、交接日志、fsync/崩溃恢复 |
| deploy/rollout-local/check.py、tests/integration/ | 真实 A/B/C 进程、长流与状态交接；新增 Linux/systemd 验收脚本 |

现有领域数据格式和业务 API 在第一版保持一致；优先改变进程组织与所有权协议，避免同时引入不必要的业务协议变动。

## 11. 分步实施与完成标准

| 阶段 | 交付内容 | 完成标准 |
| --- | --- | --- |
| P0：Linux 进程交接原型 | 两份完整测试产物，共享数据/管理/控制 listener；最小提交记录和 launch；MainPID 交接 | 真实 systemd 上 A→B→C；A 退出不重启/误杀 B；stop 清理两版；各断点冷启动选择正确 |
| P1：单进程运行与统一状态入口 | GatewayProcess、本地业务路径、StateAccess、接纳状态拆分 | 稳态一个服务 PID；现有 HTTP/SSE/Hermes/A2A、鉴权、路由管理、状态恢复回归通过 |
| P2：可恢复状态交接 | 序列化租约、快照/增量、owner 锁、回执、提交日志、显式 Adopt | 提交各断点故障注入无双写、无重复上游执行、无额度/资源/幂等丢失；活动 B 流可跨交接结束 |
| P3：真实灰度闭环 | 5%→全量→入口排空→状态交接；双维度样本与自动回退 | 真实流量和失败回退均满足 1→2→1；长流超时保留两版；缺样本不转正；禁止第三实例 |
| P4：安装与运维 | 唯一新拓扑、unit、权限、CLI/status、冷启动恢复、清理策略 | Linux 首装、正常升级、进程崩溃、机器重启、stop/start、卸载保留数据后重装通过 |
| P5：实际业务与容量 | 真实 Agent 联调、压力/长流、屏障和资源测量、操作文档 | 存量资源续接及 Agent 归属正确；交接预算有实测依据；完成连续成功、失败回退后再成功发布 |

顺序为 P0 → P1 → P2 → P3 → P4 → P5。P0 先证明无需常驻 supervisor 的生命周期可行；P2 是自动提交的硬前置。所有阶段完成前，当前 install 保持已有行为，不把未验证的完整网关灰度接到默认安装路径。

### 11.1 必测场景

| 编号 | 场景 | 必须观察到的结果 |
| --- | --- | --- |
| T01 | 两入口并发首次请求同一会话 | 一个网关版本归属、一个 Agent 归属；调用方隔离不变 |
| T02 | 5% 阶段客户端持续建立新连接 | B 不直接 Accept 公共连接，只处理被统一分配的请求 |
| T03 | HTTP keep-alive 承载多个会话；全量后旧会话发新请求 | 遵守会话/流量 revision；Agent 不改变；不能以连接比例冒充会话比例 |
| T04 | B 业务成功但没有 B 公共入口成功样本 | 不提交发布 |
| T05 | SSE 首帧成功后断流、协议内错误、回执丢失、零流量 | 正确记录 failure/unknown/excluded，不伪造成功 |
| T06 | 失败 POST、模型调用、工具操作 | 上游调用计数为一次；自动回退不重放原请求 |
| T07 | A 直接执行与 A 转发到 B 的长 SSE | 两类都阻止 A 退出；排空超时保留两个 PID |
| T08 | B 在状态交接时持有活动 SSE、后台资源和租约 | 配额/引用连续；结束后只释放一次；不要求所有 B 请求归零 |
| T09 | 状态已提交但回执丢失，跨 epoch 重试 | 返回原结果；不会假冲突、重复修改状态/扣额度或永久错误切代 |
| T10 | 慢 discovery、后台核查在屏障边界返回 | 封存后不能出现 A 的迟到写入；超时调用不越过新的 owner |
| T11 | 快照超限、增量溢出、revision 缺口、摘要损坏 | 交接前拒绝；无截断导入或隐式放宽预算 |
| T12 | PREPARED、SEALED、让锁、COMMITTED 各位置杀死 A/B；写入/fsync 失败 | 按唯一持久决定恢复，无双写，不用旧快照覆盖新业务数据 |
| T13 | A rollback 与 B commit 并发 | owner 锁串行确定 ABORTED 或 COMMITTED；不能两个结果都成功 |
| T14 | COMMITTED 后回执丢失、MainPID 通知丢失/延迟、旧父管道 EOF | 冷启动仍为 B；不错误回滚；B 不因正常父退出而死亡 |
| T15 | 控制 socket 交接、CLI 中断/重连、重复 install | 原任务继续，控制路径不被旧版 unlink，不创建额外候选 |
| T16 | 连续 A→B→C，失败回退后再次升级，systemd stop/restart/boot | 当前版本、MainPID、写入者正确；无残留 PID、僵尸和 FD |
| T17 | 伪造内部头、持有者身份、快照路径、发布产物路径 | 在副作用发生前拒绝；公共管理 Token 不能触发任意程序执行 |

代码阶段执行 `go test ./...`、`go test -race ./...`、`go vet ./...`，并扩展真实进程脚本。Linux/systemd 测试覆盖项目声明支持的系统版本；最低版本只有通过 P0 后才能写进支持范围，不能用 macOS 测试和交叉编译代替。

P5 输出交接停顿 p50/p95/p99、状态队列拒绝数、请求失败数、两版 RSS/CPU/FD、快照/增量大小与排空耗时，并据此修订第 6.4 节预算。常驻进程减少不等于灰度峰值内存减半：两版 runtime 和导入快照会同时存在。

本次文档交付不代表上述功能或验收已完成；实现后的操作说明与验证记录需分别更新 DEPLOYMENT_LIFECYCLE、ROLLOUT_OPERATIONS 和 VALIDATION。
