# Agent 网关自动灰度升级实施方案

日期：2026-10-05；实现更新：2026-10-06。P0–P4 的核心代码和本机验证已接入，Linux systemd/真实模型/生产压力验收仍待完成。当前接口、部署差异及限制见 [操作说明](ROLLOUT_OPERATIONS.md)，下文保留原实施计划。

本次实际改造步骤、代码位置、会话映射持久化机制和验证证据见 [实现过程记录](ROLLOUT_IMPLEMENTATION_RECORD.md)。

通用架构、结果协议和自动发布规则见 [HTTP 网关同机自动灰度升级设计](HTTP_GATEWAY_ROLLOUT_DESIGN.md)。本文件说明如何在 aiagent-gateway 落地，并保持通用模块可用于另一个 HTTP 网关。

## 1. 最终使用体验

用户将新二进制复制到服务器后执行：

```sh
sudo ./aiagent-gateway install
```

已完成架构迁移的机器自动执行：旧版继续服务→另一端口启动新版→5% 新会话灰度→验证真实代理请求→100% 新流量→观察→旧版排空→完成。失败自动恢复旧版承接新流量，并通过非零退出码和报告说明原因。

普通发布无须人工调权、确认转正或手工修改 systemd。`rollout status`、`resume`、`rollback` 用于查看和异常处理。首次安装没有可回退版本；首次从当前单进程架构迁移需要维护窗口，见第 9 节。

## 2. 现状与差距

| 现有代码 | 当前行为 | 本次改造 |
| --- | --- | --- |
| [cmd/gateway/main.go](../cmd/gateway/main.go) | 双监听、固定子命令、20 秒停机窗口 | 增加角色与发布命令；业务排空与进程停机分开 |
| [internal/service/service.go](../internal/service/service.go) | 停止旧服务后覆盖二进制；控制命令最长 2 分钟 | 不可变版本目录、双槽位、后台发布任务及恢复 |
| [aiagent-gateway.service](../internal/service/units/aiagent-gateway.service) | 单实例、固定路径、30 秒停止窗口 | 稳定基础组件 unit 和 worker 模板 |
| [internal/gateway/runtime/gateway.go](../internal/gateway/runtime/gateway.go) | 单进程目录锁、配置写入、健康与 shutdown 共用标记 | 状态服务独占存储；worker 读取快照；独立接纳状态 |
| [internal/gateway/runtime/scheduler.go](../internal/gateway/runtime/scheduler.go) | 进程内会话、调度和并发限制 | 共享原子归属、执行租约和全局容量 |
| [internal/gateway/runtime/resource/directory.go](../internal/gateway/runtime/resource/directory.go) | 内存资源/别名/幂等预留 | 单写者元数据服务、持久化、交接与恢复 |
| [internal/gateway/runtime/admin.go](../internal/gateway/runtime/admin.go) | 本进程配置 CRUD、资源观察及少量累计指标 | 统一管理入口、跨 worker 状态和发布报告 |
| [internal/gateway/runtime/proxy.go](../internal/gateway/runtime/proxy.go) 等代理处理器 | HTTP/SSE/A2A 转发与协议校验 | 上报统一结果，区分成功、失败、排除和未知 |
| [internal/gateway/runtime/background.go](../internal/gateway/runtime/background.go) | 本进程后台核查资源 | 单所有者或带 fencing 的核查租约 |

已有 Endpoint 权重及 active/draining/disabled 管理后端 Agent，不管理网关版本。新增发布实例模型单独命名为 Worker/Release，不能把 A/B 伪装成 Agent Endpoint。

本次状态共享会扩展 [现有设计](../DESIGN.md) 的单进程内存约束，并包含原计划 P6 的资源持久化前置工作。不能只改安装脚本就宣称满足完整需求。

## 3. 目标部署

| 角色 | unit 建议 | 监听 | 普通 install 是否更新 |
| --- | --- | --- | --- |
| 稳定入口 | `aiagent-gateway-entry.service` | 原 `GATEWAY_LISTEN`，默认 :8080；控制 UDS | 否 |
| 发布控制器 | `aiagent-gateway-rollout.service` | root 控制 UDS | 否 |
| 状态及管理服务 | `aiagent-gateway-state.service` | 公共管理端口，默认 :9090（所有网卡）；状态 UDS | 否 |
| 业务实例 A | `aiagent-gateway-worker@a.service` | 127.0.0.1:18080、19090 | 空闲槽位时更新 |
| 业务实例 B | `aiagent-gateway-worker@b.service` | 127.0.0.1:18081、19091 | 空闲槽位时更新 |

同一二进制提供拟议角色 `entry`、`rollout-controller`、`state-server`、`worker`；无子命令的现有前台单进程模式保留供开发使用，不宣称支持自动灰度。基础组件绑定基础版本路径，worker 绑定业务 release 路径。

保留 `/etc/aiagent-gateway/gateway.env` 的凭据及外部地址配置。worker 使用独立 slot 配置覆盖内部监听、状态地址和 release 身份，`GATEWAY_PUBLIC_URL` 始终是外部地址，不暴露内部端口。

`start/stop/status` 扩展为管理整套服务并列出各角色状态；日常升级使用 `install`。`restart` 明确属于有中断可能的运维操作，不能伪装成灰度发布。卸载需协调正在进行的发布并停止全部角色，继续保留凭据、状态和审计数据。

## 4. 代码模块与协议边界

```text
internal/releaseprotocol/       # 产品无关 DTO、manifest、结果与控制协议
internal/rollout/               # 状态机、策略、发布日志、恢复和报告
internal/entry/                 # 固定入口、CAS 路由、流式转发及传输观察
internal/service/              # 安装、权限、systemd driver、unit 嵌入
internal/state/                # 单写者资源/配置服务与日志快照
internal/gateway/               # Agent 协议与业务逻辑，提供 rollout adapter
cmd/gateway/                   # CLI 和角色组装
```

通用前三个包不得导入 `internal/gateway`，通过接口或本机协议调用适配器。`internal/state` 的资源模型属于本产品，另一个无状态 HTTP 网关无需复制。

首版定义领域操作而非远程 KV：`GetConfigSnapshot`、`ReserveOperation`、`ResolveResources`、`AcquireExecution`、`CommitObservation`、`ReleaseExecution`、`TransferAffinity`、`GetDrainStatus`。每个变更带操作 ID、worker incarnation、预期 generation 和配置 revision，支持重试去重及迟到请求拒绝。

网络调用期间不持有跨模块生命周期锁；先取得不可变快照/租约，再访问网络，最后按 generation 提交结果。不能把现有持锁临界区直接改成锁内 RPC，避免死锁和全局阻塞。

## 5. 状态共享与持久化

### 5.1 配置唯一写入

由状态服务独占 `/var/lib/aiagent-gateway/shared`，负责路由配置、管理幂等和审计；worker 不能打开该目录现有的独占写锁，更不能各自写 config.json。

保留管理 API 的 ETag、If-Match、幂等及凭据隔离语义。配置更新校验跨 worker 引用，而非只看本进程。worker 通过快照版本与订阅更新配置，接流量前确认同一 revision；worker 本地文件仅用于缓存和运行数据。

首版发布活动期间冻结路由/Endpoint 配置及凭据变更，返回 `409 ROLLOUT_IN_PROGRESS`，CLI 和报告明确展示冻结原因。资源读写、正常业务请求、任务查询/取消及发布控制继续可用。发布完成或安全回退后解除冻结；异常待恢复期间保持冻结，避免验证基线和回退兼容性漂移。多版本在线配置写入可在后续以快照屏障方式扩展。

### 5.2 必须共享的业务状态

| 状态 | 要求 |
| --- | --- |
| generic 会话 | caller/route/interface/session→原 Agent Endpoint 和 worker 归属 |
| Hermes session/run/response | 公开 ID、上游 ID、父子引用、别名、调用方隔离、生命周期 |
| A2A context/task | 同上，跨 worker 查询/取消/订阅仍定位原实例 |
| 数据创建幂等 | operation key、指纹、预留、未知结果和已确认资源 |
| 列表游标 | 共享快照或可由两版解析的受保护游标；切流不导致混页或越权 |
| 并发额度 | 网关/caller/Endpoint 的合计执行额度，避免双实例使限制翻倍 |
| 版本亲和性 | caller/route/interface/逻辑链→worker、incarnation、generation |
| 后台核查/执行 | 单所有者或租约，跨版本不重复执行 |

Endpoint 身份、worker 版本归属和请求执行租约分开：转移 worker 不改变原 Agent Endpoint，也不重建上游任务。不同协议现有的隔离边界保持不变，不因此承诺跨协议共享同一会话。

### 5.3 本机日志与快照

为保持 Go 标准库部署方式，首版采用单写者的追加日志加周期快照，而非引入 Redis 或多个进程同时改 JSON：

1. 资源预留及幂等操作先写入持久日志，提交成功才允许上游副作用。
2. 发出有副作用的请求前持久化“可能已发送”的状态；崩溃后将其视为 unknown，不能重发创建。
3. 公开资源 ID、别名及映射在对客户端可见前完成持久提交。
4. 日志包含格式版本、序列、长度和校验；组提交须在 fsync 成功后才确认各操作。
5. 快照采用临时文件、文件 fsync、rename、目录 fsync；仅在快照确认后回收对应日志。
6. 恢复时允许识别并清理未提交的不完整尾记录；中间损坏、未知格式或无法确认一致性则拒绝 ready。

不能每个 token 重写状态文件。持久化记录资源创建、标识关系及生命周期变化；高频事件合并，但必须遵守公开 ID 提交和未知执行保护。心跳及瞬时连接计数与可恢复元数据区分，恢复后不能照搬过期 ActiveRefs。

该存储不是可省略的辅助工作：必须覆盖崩溃窗口、磁盘满、迟到写入、日志回收、容量上限和恢复后的状态核查，并单独测量同步持久化对吞吐与延迟的影响。

### 5.4 进程故障与交接

计划内交接按逻辑链执行：冻结源 worker 该归属的新执行→等待现有执行及状态提交完成→记录目标 worker 并递增 generation→允许目标接纳。请求可以有界等待或在上游执行前收到明确重试响应。

异步任务已在 Agent 运行且当前无本地执行时，可迁移后续查询/取消的处理归属；保留原 task/run ID 和 Endpoint，不能再创建一次。仍由 worker 后台线程驱动的操作必须等待线程退出或实现明确接管，不能仅看 HTTP inflight=0。

worker 意外退出后，用 incarnation 隔离其迟到事件；已发送且结果未知的操作维持 unknown，通过上游查询核查。租约过期不等于 Agent 任务终止，不能直接释放所有资源并容许重复创建。

状态服务重启后恢复资源、幂等和归属，重新登记存活 worker 并核对租约；未确认的执行额度保守处理。无法访问状态服务时禁止新的资源创建/调度，已有流可在原有效执行约束内继续传输，但不能在元数据无法提交时向客户端宣布新资源成功。

## 6. Agent 请求适配与真实验证

### 6.1 入口分类与版本分配

复用现有协议解析规则提取身份、route、interface 及标准资源引用，生成不含 Token 的可信亲和键。入口需要解析 JSON 时复用有界解析器并保留原始请求，不扫描或改写业务 parts、metadata、工具结果。

路径覆盖 `/agents/{key}`、兼容 `/proxy/{key}`、`/a2a/{key}`。已知 session/run/response/task/context 通过共享索引解析到逻辑链；同一请求多个引用冲突时沿用现有拒绝语义。创建操作先检查幂等预留，重复键不能重新参与灰度选择。

仅新逻辑链或明确无状态请求按 5% 分配候选，既有归属保持版本粘性。新增灰度字段不接受客户端任意指定 release；诊断探测只能通过受保护的控制接口选择目标。

### 6.2 结果采集改造位置

| 文件/模块 | 需要上报的信息 |
| --- | --- |
| `proxy.go` | 实际是否访问上游、HTTP 读写完整性、超时/取消/状态分类 |
| `hermes.go`、`hermes_observe.go`、`hermes_resources.go` | 原生 HTTP 语义、公开资源提交结果、异步创建确认 |
| `hermes_stream.go` | 协议终态、错误事件、提前 EOF、下游写失败 |
| `a2a.go`、`a2a_codec.go` | RPC result/error、格式及资源归属校验结果 |
| `a2a_stream.go`、`hermes_a2a.go` | 流式完成、后台执行、首 Task 和订阅生命周期 |
| `entry` | 请求的实际目标、最终下游传输状态、结果关联 |

现有 `gateway_requests_total`、`gateway_failures_total` 保留兼容，但不作为唯一发布判据。新增发布专用的有限维度指标：请求最终 verdict、profile、release、reason，以及活动连接、后台执行、待交接归属、实际流量分布。

重点防止四种误报：只看到上游 200 就成功；首 SSE 事件就成功；返回公开 ID 但映射未提交；HTTP 已结束但 worker 后台操作被当成排空。

### 6.3 本项目默认验证策略

默认使用通用方案的 5% 灰度、最少 60 秒、最多 300 秒、每个必需 profile 至少 10 个成功结果、有效失败阈值 0、全量观察 300 秒、排空上限 600 秒。所有时间和样本参数可配置，且与原路由请求超时独立。

必需 profile 根据启用路由和发布策略确定：普通 HTTP、SSE、A2A JSON、A2A SSE 分别验证，不能相互代替。支持 streaming 的路由若要纳入自动发布保证，须提供可验证的结束语义与实际样本；无限流或低频接口未取得样本则自动回退并说明不足。

灰度真实请求不做镜像复制。若配置真实 Agent 冒烟链路作为补充验证，需明确调用方、路由、操作、上限及清理方式；不能自动使用业务任务重复调用模型。异步任务创建的代理成功与模型任务最终成功分开统计。

报告至少包含：新旧版本及哈希、配置版本、各阶段起止时间、必需与实际 profile、每类成功/失败/排除/未知数量、未覆盖路由、回退原因、入口实际 revision、旧新实例状态及残留工作数量。

## 7. install 和 systemd 改造

安装器先校验及暂存产物，再向后台发布控制器提交任务；当前统一的两分钟 context 改为短提交超时与独立的进度等待，不能在验证中途因 CLI 超时停止所有服务。

具体实现：

1. manifest 编译进二进制，包含 release ID、构建信息、协议范围及能力；暂存后核对内容哈希和目标架构。
2. 保留安装互斥锁，后台另持有当前发布租约；并发 install 返回当前任务 ID。
3. 使用 worker 模板及每槽位配置，确保 systemd 自重启仍启动相同 release。
4. 初始化只由状态服务执行；worker 启动不创建空配置，不接管共享目录写锁。
5. readiness 校验 worker identity、配置 revision、共享服务和 profile 能力，候选探测不得误命中旧实例。
6. 发布控制器根据结果协议推进状态，不解析日志文本决定转正。
7. 先经控制接口完成业务排空，再调用 systemctl stop；沿用的短 `TimeoutStopSec` 只覆盖最后进程退出阶段。
8. 候选回退后保留失败报告及产物；有引用的 release、正在使用的槽位和共享状态禁止自动清理。
9. 自动回退只恢复运行版本与流量，不恢复一份旧 config.json 覆盖共享状态；灰度中新建的合法资源及幂等记录必须保留。

普通 install 不更新稳定组件。新业务要求更高 state/control 协议，或生成旧版不能理解的资源类型时，预检查直接拒绝，不先接流量再测试回退是否可行。

## 8. 分阶段交付

| 阶段 | 主要工作 | 出口条件 |
| --- | --- | --- |
| P0：协议与接口 | DTO、manifest、结果分类、模块边界、适配器及状态接口 | 通用核心无 Agent 依赖；状态/结果契约可用于第二网关 |
| P1：多实例运行基础 | 不可变版本、worker 模板、稳定入口、CAS 路由、控制器日志及恢复 | Linux 上两版并行，候选失败不停止旧版；仅测试环境启用切流 |
| P2：状态共享与恢复 | 配置单写者、资源/幂等持久化、全局配额、游标、亲和与租约 | 两 worker 并发与重启不破坏隔离/归属；可安全交接/回退 |
| P3：结果协议与业务适配 | HTTP/SSE/Hermes/A2A 完整判据、入口合并、去重及缺口处理 | 200 错误封装、半流、未知结果都不能误转正 |
| P4：自动安装发布 | 完整状态机、门槛、100% 观察、排空、自动回退、CLI 报告 | 一次 install 完成成功/失败两条闭环，终端退出不影响控制 |
| P5：迁移与真实验收 | 老架构迁移、Linux 故障注入、真实双 Agent、压力及运维文档 | 完成验收矩阵后才默认启用生产自动灰度 |
| P6：第二 HTTP 网关接入 | 实现其 adapter/profile、替换产品目录和 unit，按需接状态 | 复用核心不增加 Agent 分支，运行同一套通用发布验收 |

P1 完成不等于功能交付。P2 是当前有状态网关自动转正的硬前置条件；P3/P4 可以基于模拟适配器开发，但默认 `install` 必须在 P2–P5 全部通过后才切换新行为。这里的阶段编号只属于本方案，与旧 Hermes 开发计划编号独立。

## 9. 首次迁移与兼容性

当前版本没有运行时状态导出/交接协议，公共端口也由业务进程直接占用。首次进入新架构不能承诺已有内存会话透明保留。

建议流程：在维护窗口停止接收新业务，按现有能力等待请求结束；保存凭据、原始配置及旧产物；停止旧进程；将原配置导入共享存储并保持 route/Endpoint ID；启动基础组件及第一个 worker；核对公开地址与业务探测后开放入口。

当前版本的旧公开 session/task 等映射在重启后失效是既有边界，迁移说明需明确，不能把“配置保留”写成“会话全部保留”。如果必须保留迁移前运行时资源，需要先发布能导出并冻结状态的过渡版本，作为独立工作交付。

迁移失败时，在没有向外开放新架构写入前可恢复旧布局；已经产生新架构资源后，不能恢复旧快照假装无损回滚，应停止放量并按状态兼容流程处理。

后续正常发布要求新旧 worker 兼容当前基础服务协议。控制、状态及配置格式采用显式版本范围；不兼容升级走独立迁移，不受自动灰度回退承诺覆盖。

## 10. 验收与交付标准

### 10.1 自动测试

- 运行现有 `go test ./...`、`go test -race ./...`、`go vet ./...`，保持既有协议和安全边界。
- 通用状态机使用可控时钟、伪 process driver 和持久化故障点覆盖所有状态转移，不让测试实际等待五分钟。
- 入口使用真实 HTTP/SSE 模拟上游验证 keep-alive 切流、半响应、客户端取消和不重复 POST。
- 状态服务验证并发首请求、跨 caller 隔离、幂等冲突、fencing、worker 重启、全局额度及分页连续性。
- 每个日志/快照提交边界注入崩溃或磁盘失败，验证恢复后不丢已确认映射、不重发未知创建。
- 结果协议验证重复、乱序、缺失、晚到失败、候选 incarnation 变化和不同 profile 样本隔离。

### 10.2 Linux systemd 实机

必须覆盖全新安装、正常升级、候选无法启动、候选 ready 但代理失败、全量后失败、安装终端断开、控制器被 kill、机器重启、端口占用、排空超时和并发 install。macOS 测试与交叉编译不能替代此项。

确认自动升级期间入口 PID/监听保持稳定，业务 worker 确实为不同 release，自动回退后的新流量到旧 worker；检查失败报告、退出码和 systemd 状态相互一致。

### 10.3 Agent 真实链路

用两个真实 Hermes Endpoint 验证：旧 HTTP session 续接、run 查询/停止、response previous_response_id、A2A context/task 查询/取消/订阅、列表游标、调用方隔离、同键幂等，以及灰度和回退期间 Agent Endpoint 归属不变。

升级过程中保持长 SSE，请求结束前旧 worker 不退出；模拟候选在创建操作发出后失败，确认没有重复创建。等待中的审批和未知任务不能被排空逻辑误判为已结束。

### 10.4 发布完成标准

以下全部满足才能标记实现完成：

1. 一次 install 自动实现验证、切流、观察、排空，失败可自动回退并准确报告。
2. 必需 profile 的真实代理结果决定发布，样本不足和结果未知不被当成成功。
3. 灰度/回退期间旧资源可按原 Agent 归属访问，副作用请求不重复执行。
4. 所有终端状态与实际流量、实例和存储状态一致，异常状态可恢复。
5. README、DESIGN、DEVELOPMENT、管理契约和 VALIDATION 随代码更新实际能力与已验证边界。
6. 第二网关接入指引和通用验收可独立使用；第二产品尚未实测时不得宣称已完成跨产品验证。

实现记录：已新增通用 releaseprotocol/entry/rollout、共享资源 RPC 和日志、worker 结果采集及 systemd 安装入口。当前采用共享元数据配合请求边界切换，进行中的连接留在旧 worker；没有独立的按资源迁移进程。unit 使用 worker-a/worker-b 的确切版本路径；历史方案中的其他拟议接口不全部代表已实现接口。当前测试范围见操作说明和 VALIDATION。
