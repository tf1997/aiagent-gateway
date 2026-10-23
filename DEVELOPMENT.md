# 开发文档

当前主线为 [完整网关灰度方案](docs/SINGLE_PROCESS_ROLLOUT_PLAN.md)，交付与未完成项以 [实现进度](docs/SINGLE_PROCESS_IMPLEMENTATION_STATUS.md) 为准。默认安装已接入 1 → 2 → 1 自动发布与 Linux/systemd 接管。HTTP/SSE/Hermes/A2A 协议实现沿用既有模块，后续章节保留历史开发记录，不应据旧 host/worker 描述配置当前部署。

## 1. 范围和约束

以 DESIGN.md 为架构依据，用 Go 1.24+ 实现（YAML 解析使用 go.yaml.in/yaml/v3）单节点网关。Route 直接内嵌 Endpoint，不引入独立服务池、数据库或 Redis。配置从 HTTP API 更新并原子落盘；普通前台入口的会话/资源在内存；完整 process / systemd 入口持久化业务元数据，在线交接还保留活动租约和短期缓存。

交付可运行的首版：独立管理/数据监听、Bearer 认证、路由和实例 CRUD、配置版本/ETag、创建幂等、原子文件恢复、HTTP/SSE 代理、加权调度、会话绑定、健康检查、排空和优雅停机。A2A 仅支持 1.0 JSON-RPC HTTP/SSE，方法为 SendMessage、SendStreamingMessage、GetTask、ListTasks、CancelTask、SubscribeToTask；不支持推送通知。规范固定到 https://github.com/a2aproject/A2A/tree/v1.0.1 。

## 2. 模块与开发顺序

| 文件或目录 | 职责 |
| --- | --- |
| cmd/gateway/main.go、internal/appconfig | YAML 配置、启动入口、信号和停机 |
| internal/process/、internal/handoff/ | 完整网关、自动发布、监听与控制交接、独占 owner 和持久决定 |
| internal/service/ | 不可变产物、launch 选版、安装与 systemd unit |
| internal/gateway/gateway.go | 对外入口和类型别名，保留 gateway.Open 等调用方式 |
| internal/gateway/runtime/ | 启动组装、调度、会话绑定、健康检查和跨模块协调 |
| internal/gateway/runtime/config/ | 配置校验与快照、管理 CRUD、消费者认证、迁移和原子存储 |
| internal/gateway/runtime/resource/ | 资源目录、别名和父子关系、幂等预留、生命周期及持久化 |
| internal/gateway/runtime/shared/ | Unix Socket 通信、请求重放与代次校验、租约及列表缓存 |
| internal/gateway/runtime/rollout/ | Agent 归属解析、worker 身份和发布结果观察 |
| internal/gateway/a2a/ | Card、A2A 1.0 编解码与资源映射、任务分页、SSE 和 Hermes A2A 兼容 |
| internal/gateway/hermes/ | Hermes HTTP 请求、会话分页、响应/事件观察及后台查询 |
| internal/gateway/protocol/ | 共享模型、适配器操作接口、资源/缓存结构和 HTTP 工具 |
| tests/integration/ | 通过公开入口验证 HTTP/A2A/Hermes、认证、存储恢复和跨 worker 行为 |
| api/openapi.yaml | 管理接口契约 |

依赖方向为 gateway → runtime → a2a/hermes → protocol。runtime 组装内部四个子包，子包通过操作接口取得跨模块能力，不反向导入 runtime；资源模块封装自身的锁和持久状态。适配器继续通过 protocol 访问调度、资源、缓存和传输。公开接口的集成测试及 A2A fixtures 位于 tests/integration；依赖私有状态的测试与配置迁移 fixture 保留在 runtime，独立资源测试随资源模块存放。详细导航见 [Gateway 代码结构](internal/gateway/README.md)、[Runtime 代码结构](internal/gateway/runtime/README.md)和 [集成测试说明](tests/integration/README.md)。第 7–16 节为阶段记录，其中旧文件名对应此处整理前的结构。

## 3. 一致性与并发

配置通过 atomic.Pointer 发布，只读 Map 和切片不可原地修改。管理更新持配置写锁和生命周期写锁，复制候选配置，检查引用，先持久化 `.config.pending.json` 恢复事务，再以同目录临时文件、fsync、rename、目录同步分别提交 `routes.json`、`route-logs.json` 和仅含版本的 `config.json`，清理事务后发布。事务提交之前失败保留旧配置；之后写入失败进入不可用状态，重启重放并校验恢复。目录使用操作系统独占文件锁，初版支持 Linux/macOS。

新绑定先持生命周期读锁，再加载快照、获取会话分片锁和路由调度锁，所有路径保持相同顺序。网络请求期间不持这些锁。运行状态由稳定的 routeID/endpointID 索引，配置更新不迁移有效绑定。A2A 标识映射须先完成再向客户端输出。

`POST /admin/v1/routes` 按正文 key 创建或更新：创建返回 201，更新返回 200。更新采用 JSON merge patch 语义保留未提供的路由字段；提供 endpoints 时整体替换列表，同名实例保留 ID，未命名实例仅按完全相同的 interfaces 匹配，歧义拒绝。所有被移除或改址的实例仍检查引用。GET/DELETE 路径支持 key，优先于兼容内部 ID；POST 更新和按 key 删除可选 If-Match。原 PATCH、实例 DELETE、按内部 ID 删除路由仍要求 If-Match；路由 PATCH 不接受 endpoints。ID、版本不可改。POST 支持 Idempotency-Key，同键不同原始请求返回冲突；重放返回原状态码和响应，不重复更新。审计与幂等记录有界。

## 4. 认证与运行配置

YAML 文件提供监听端口、数据目录和管理 Token；消费者通过路由 consumers 动态登记，只持久化 Token 哈希。消费者 ID 是资源隔离键，每条路由独立授权。上游 Token 直接保存在路由接口的 token 中，代理移除客户端 Authorization、Cookie 和不可信身份头后注入可信身份/上游凭据。

启动时自动初始化缺失的路由存储，已有文件直接读取；损坏或不可读时拒绝启动。ready 要求有效快照及持久化健康。数据和公共管理端口默认监听所有网卡（`:8080`、`:9090`），管理请求使用管理 Token 认证。完整候选的执行与状态通信使用私有 Unix Socket；发布控制额外核对 Unix peer UID。

## 5. 验收与测试

- 管理：嵌套创建、PATCH、ETag 冲突、实例归属、幂等重试、重启恢复、进程锁、持久化失败。
- 代理：路径/查询串、状态码、流式及时输出、客户端断开传播、超时、凭据隔离。
- 会话：首次并发一致、不同调用方隔离、扩容/权重更新不迁移、排空、故障不切换、引用保护、TTL/容量。
- A2A：消息、任务、context 映射、流式事件、查询/取消、权限隔离、未知任务拒绝、不支持能力拒绝。
- 执行 go test ./...、go test -race ./...、go vet ./...；微基准使用 go test -bench . -benchmem，真实容量数据需在目标机器单独压测。

## 6. 交付状态

实现和验证结束后更新 README 的实际能力、运行示例与已知限制。未经过真实 A2A SDK/Agent 验证的部分必须标明，不把本地协议模拟测试视为互操作认证；生产性能指标以压测报告为准。


## 7. 首版实现记录

已实现上述模块及管理 OpenAPI，运行方法见 README。路由和实例查询、条件更新、幂等创建、持久化失败保护、会话绑定及排空均有自动测试。A2A 使用原生 JSON 处理而非外部 SDK，限制在本文声明的 0.3.0 JSON-RPC 方法内。

首版运行状态与设计的差异：调用方 ID 承担隔离键，尚无独立租户/RBAC 管理；健康检查地址配置在实例 `health_url`；默认每 10 秒串行主动检查；配置幂等和审计按最近 1000 条保留；A2A 映射使用独立互斥锁，尚未分片；OpenTelemetry 和延迟指标待补充。以上是后续工程项，详见 README 的运行边界。

验证命令、结果与未完成的外部验证见 VALIDATION.md。性能微基准只衡量会话绑定热路径，不代表包含网络、TLS 和上游调用的 QPS。


## 8. 内置 systemd 服务管理

`internal/service` 管理系统安装与服务生命周期；unit 通过 `go:embed` 编译进二进制。入口提供 install/uninstall/start/stop/restart/status 子命令，无参数仍以前台方式运行。仅 Linux 支持服务管理；除 status 外需要 root。命令通过 os/exec 直接传递参数，不调用 shell 或执行凭据文件。

安装使用文件锁避免并发变更。先读入候选二进制和备份，再停止旧服务、原子替换二进制及 unit；启动失败恢复旧文件并尝试恢复原来的运行状态。首次创建独立管理凭据，升级不修改凭据和数据。安装时调用一次性初始化 unit；常规前台和状态服务启动也会自动初始化缺失的路由存储。启动检查通过 ExecStartPost 调用 --check-ready；主进程监听失败返回非零退出码，使 Restart=on-failure 生效。

`--init-only` 完成配置创建或验证后退出，不监听端口；`--check-ready` 根据配置的数据监听地址访问本机 /readyz，最长等待 15 秒，不打开配置存储锁。服务默认预留 30 秒停止时间，覆盖网关 20 秒优雅停机窗口。

控制流程测试使用临时文件系统和替代的命令执行器，验证首次安装、升级、回滚、状态丢失保护、卸载保留数据和嵌入资源；不调用真实 systemctl 或修改系统账户。真实 Linux 集成需验证 service 用户权限、LoadCredential 读取 YAML、重启/开机启动、端口冲突及进程异常后的重启行为。

## 9. P1：配置模型与统一入口（2026-09-30）

- Route 使用 `agent_type`/`expose`，Endpoint 使用 `interfaces`。旧字段仅存在于 `config_migration.go` 的 schema 1 DTO 和兼容输入正规化中。
- schema 1 启动时内存迁移；首次管理写入前原子发布原始配置备份，随后沿用原子配置提交。旧幂等回执不重建、不改指纹。
- `/agents/{appId}` 统一分发，Card 宣告规范 RPC 地址；旧路径复用入口认证、expose、限流和会话绑定。保留路径优先匹配。
- 普通会话绑定键增加入口维度，调度按接口健康分开，Endpoint 共享 HTTP 并发上限。Endpoint 详情增加 `runtime`，按接口展示健康和并发。
- 接口地址、版本和移除，以及 Agent 类型/入口移除均受引用保护。解析与 acquire 之间发生配置发布时拒绝过时请求，避免向不同类型或版本的接口误发。
- 新类型和协议版本目前仅支持配置：Hermes HTTP 与 A2A 1.0 RPC/Card 返回明确的 501。该阶段尚未实现公共资源目录；P2 进度见下节。能力发现、Hermes HTTP 和官方 SDK 互操作仍待完成，不作为完整适配发布。

迁移/回滚步骤见 README，新增验证记录见 VALIDATION。测试 fixtures 中的 schema 1 配置用于迁移回归，不是 A2A 1.0 协议 fixtures。

## 10. P2：公共资源目录（2026-09-30）

`resources.go` 统一维护 Hermes session/run/response 与 A2A context/task 的正反向索引、客户端别名、谱系 ID、父子引用、状态和数据幂等预留；`a2a.go` 已移除独立 task/context Map，现有 0.3.0 JSON/SSE 走批量资源登记。generic 请求头粘性保留原分片表。

- 显式创建使用 `reserveResource` 在调度前原子查询别名及幂等指纹；仅新资源调用调度。Hermes 会话生成隔离的上游 ID，已有资源通过 `acquireResource` 固定实例并持有短期引用。fork 必须位于父实例且不允许 draining；已有会话的 Run/response 继续允许 draining。
- 适配器在网络发送前调用 `update(..., resourceUnknown)`；明确尚未发送的失败才可 `abandon`。收到上游标识后使用 `attachAlias` 登记，完成观察后更新状态。generation 校验拒绝旧请求修改已撤销资源。HTTP 传输和客户端写入均不得持有资源锁。
- 锁顺序为生命周期读锁 → 目录锁 → 调度/实例状态锁。`acquireResource` 在固定资源引用后释放目录锁，再调用已持生命周期锁的 `acquireLocked`，避免重复读取生命周期锁导致写锁等待时死锁。generic 分片锁可短期读取目录，不反向持目录锁操作分片。
- A2A 先遍历标准字段收集变更，统一检查容量、归属、父关系和引用，再批量提交并改写报文。失败时既不修改原结果，也不留下新 context/task；parts/metadata 保持不变。无状态的 artifact/message 观察不覆盖已知状态；未知状态保留归属，终态不倒退。
- 资源、别名、数据预留均有容量上限。请求引用/子引用/非终态执行保护父链；空闲回收清理全部反向索引，未知或审批等待不误清理。状态更新依赖目前 A2A 的查询及事件，主动后台核查待协议适配阶段接入。
- 管理 `/resources`、`/runs`、兼容 `/tasks` 及实例详情提供资源观察；新增三个容量环境变量和资源相关指标。面向调用方的资源列表游标随 P3/P4 接入，管理端继续采用 offset/limit。

本阶段完成公共基础和 A2A 0.3.0 接入，不开放 Hermes HTTP，也不提供 A2A 1.0 编解码；资源归属与预留仍不跨重启恢复。

内存网络竞态检查及用户手动执行的本机真实 TCP 竞态测试均已通过，具体命令和终端输出见 [VALIDATION.md](VALIDATION.md)。

## 11. A2A 1.0 升级（2026-09-30）

按用户决定替换 0.3.0，不保留旧编解码器。`a2a.go` 负责协议入口、Card、归属路由和转发；`a2a_codec.go` 校验 1.0 数据与资源批量映射；`a2a_stream.go` 实现 SSE 边界及生命周期检查；`a2a_list.go` 负责隔离的任务查询和有界快照分页。协议字段依据仓库内固定的官方 `testdata/a2a-v1.0.1/a2a.proto`；JSON fixtures 为按该 schema 编写的测试示例，来源和 SHA-256 见同目录 README。

配置仅接受 `protocol_version: "1.0"`。旧形态没有版本声明的 A2A 接口升级为 1.0；schema 2 中明确的旧版本拒绝，要求先升级上游后改配置。tenant 仅由接口配置注入，纳入已有资源引用的身份变更保护。旧 HTTP 路径别名保留，旧协议报文拒绝。

新增验证覆盖 1.0 Card/安全结构、版本协商、方法与 Part oneof、保留数字精度、task/message 响应、默认阻塞语义、标准错误、固定原实例的查询/取消/订阅、SSE 多行/CRLF/CR/首帧/任务一致性/中断/等待与终态、列表隔离/过滤/排序/快照重放/游标过期，以及上游失败不迁移。分页默认历史为 0，限制与配置迁移说明见 README。

P4 的本地协议实现已完成；后续已补齐第 12 节所列单实例 Hermes 检查，官方 SDK 和其余验收条件仍待执行。Hermes HTTP、能力发现和后台核查也尚未交付。不要将本轮变更标为全部 P1–P5 已完成。

## 12. 本机 Hermes 代理联调（2026-10-04）

已部署的 Hermes 使用独立 HTTP/A2A 端点。`deploy/hermes-local/gateway.py` 编译启动本机独立网关（18080/19090），加载已有上游凭据、生成隔离的本机调用凭据，通过管理 API 注册 `hermes-local` generic 双入口路由，并根据上游 Card 配置 A2A 1.0 流式能力。重复执行保留凭据和端点身份，路由 PATCH 使用版本条件；模型请求使用更长的响应头超时。

`check_gateway.py` 提供真实 HTTP session 同步/SSE 两轮续接、Chat Completions SSE、A2A Task/context 续接、查询分页、调用方隔离及 HTTP 并发释放检查，结果保存在忽略目录中的 JSON。脚本离线检查及 Go 竞态回归通过；用户手动执行后，本地日志和注册结果确认网关及路由启动成功。端到端检查最初遇到自动审批服务 HTTP 404，随后用户在本机执行，全部五项检查通过；代理核对了 `integration-all.json` 的 `passed: true`、两轮 recall、四条 HTTP 历史、两个 A2A 任务和零 inflight 结果。

该接入使用 generic HTTP 代理，不代表 P3 Hermes session/run/response 专用适配完成，也未覆盖多 profile、多实例、官方 SDK 或生产容量验收。运行方式见 [部署说明](deploy/hermes-local/README.md)，验证来源见 [VALIDATION.md](VALIDATION.md)。

## 13. Hermes HTTP、后台核查与 SDK 联调（2026-10-05）

`hermes.go` 负责显式路径表、请求引用校验、创建预留、实例选择和 HTTP 传输；`hermes_observe.go` / `hermes_resources.go` 只转换标准标识字段，在输出前原子检查和登记 session/run/response 归属。业务文本、工具结果和 metadata 不递归改写。路径/头/正文冲突、未知归属、失败不换实例和未发送预留回滚均有回归。

`hermes_stream.go` 复用 SSE 行解析，保留事件、心跳与重放序号，按事件观察生命周期；`hermes_list.go` 提供仅调用方可见的会话查询和有界快照游标。原生 Sessions、fork、同步/SSE 聊天、模型锁、Runs 提交/状态/events/stop/steer/approval、显式会话 Chat Completions，以及 Responses 新链/上一响应/命名会话已接入。详细兼容约束见 [HTTP 契约](docs/HERMES_HTTP_API.md)。

`hermes_capabilities.go` 按接口缓存探测结果，能力信息仅用于诊断和展示，Hermes 调度按接口健康状态判断，兼容能力不完整的低版本；管理接口同时展示健康、就绪和能力，`capabilities.ready` 不影响接口就绪。`reconcile.go` 使用独立后台循环，有界轮转查询未完成/未知资源；查询失败不当成终态，stop 的 stopping 不提前释放执行归属。

真实 Hermes v2026.9.24 忽略 SendMessage 的 returnImmediately，且 SubscribeToTask 缺少标准要求的初始 Task。`hermes_a2a.go` 分别通过同实例流式入口的有界后台读取、同实例 GetTask 首帧补全实现公开 1.0 语义；新增并发释放、初始 Task、后台完成和停止生命周期测试。generic A2A 的严格标准检查保留。

本机增加第二个独立 Hermes 数据目录及端点；官方 a2a-sdk 固定 1.2.0，完整依赖锁定在 `deploy/hermes-local/requirements-a2a-sdk.txt`。SDK Card、六个 RPC、context 续接/分页、订阅取消已在两个实例上通过。原生资源、实例故障/排空和重启恢复边界由独立脚本检查，最终结果以 [验证记录](VALIDATION.md) 为准。P6 资源归属持久化仍按原方案单独交付。


## 14. 同机自动灰度升级（2026-10-06）

实现入口见 [操作说明](docs/ROLLOUT_OPERATIONS.md)，通用设计见 [HTTP 发布机制](docs/HTTP_GATEWAY_ROLLOUT_DESIGN.md)。新增固定入口、后台发布状态机、不可变 release 目录、独立 worker 端口和共享状态服务；默认 install 在新架构下验证真实代理结果后自动切流或回退。

单进程前台模式继续使用原内存绑定；灰度 worker 通过本机 RPC 共享路由/资源/幂等/并发额度。资源与会话日志由单写者提交，公共 ID 在持久提交后输出。普通业务升级不重启状态服务；状态代次变化会拒绝旧 worker 写入。短期列表快照跨 worker 共享，但不跨状态服务重启。

通用状态机和入口不依赖 Agent 包；systemd driver 及 Agent 适配位于本项目。安装与迁移仍需 Linux 实机验收，不以本机控制流程模拟或交叉构建作为已部署证明。

## 15. YAML 进程配置（2026-10-11）

启动配置统一由 internal/appconfig 严格读取 YAML，默认 config.yaml，显式 --config 指定路径。所有前台角色、init-only 和 check-ready 共用该入口；不再读取 GATEWAY_* 或上游凭据环境变量。相对路径基于配置文件目录，路由与上游 Token 由管理 API 维护；YAML 不接受 credentials。

安装器仅生成 root 专属管理配置 config.yaml，systemd 使用 LoadCredential 提供私有副本；新部署清单 version 为 2，拒绝将 YAML worker 直接发到旧环境变量基础组件。配置文件、unit 和旧部署维护迁移见 [配置说明](docs/CONFIGURATION.md)。

## 16. 动态路由消费者（2026-10-11）

YAML callers 和 Options.Callers 已移除。POST 创建/upsert 及兼容 PATCH 接受 consumers；省略保留、数组整体替换、空数组撤销。每项 id 为稳定 owner，token 仅为写入字段；保存 SHA-256 哈希，管理响应和幂等回执只包含 ID。新建无消费者路由默认拒绝业务访问。

数据面与共享状态入口基于目标路由快照认证，worker 每次请求刷新配置；配置成功提交后新 Token 立即生效。轮换保持 owner，不迁移会话/任务；动态并发计数保留活跃请求并在归零时回收。撤销不取消既有执行，后台核查可继续使用持久资源的 owner，但不会恢复其公开访问权限。


## 17. 稳定宿主与业务子进程（2026-10-19）

`internal/host` 将 entry、共享状态及 rollout 控制器组装为同一个非 root 宿主；入口解析、freeze/retire/lease count 使用本地适配器，worker 继续使用既有状态 UDS 协议。`internal/service/child.go` 管理两个槽位的真实子进程，验证只读产物 SHA-256、PID 与 incarnation，等待 readiness，排空后停止并 Wait 回收。父进程生命管道关闭后 worker 退出，避免脱离宿主管理。

安装统一采用 host-worker，稳定期 2 个、灰度期 3 个进程。部署清单仅包含 infrastructure、initial_release 和 policy，没有拓扑版本字段。已移除旧角色 CLI、systemd worker driver、旧单进程安装器及 rollout_data。宿主更新命令为停机后的 `install --update-host`：先备份配置、状态、CLI、unit 和 drop-in，保留业务归属及 drop-in，归档旧进程身份，建立新基线。root 安装器暂存产物，host 控制 UDS 校验 peer UID 和管理 Token。

宿主启动先核对发布报告与 entry 切流意图；未提交灰度回到旧稳定版，COMMITTING 恢复已选定的新版本。旧日志不能直接使入口 ready，必须等新子进程恢复。人工停机先排空入口，systemd KillMode=mixed 为最终 cgroup 清理兜底；正常 worker 发布不改变 host PID、状态 epoch 或公共端口。

验证涵盖真实 2 → 3 → 2、A → B → C、失败回退、SSE 排空超时与恢复、worker/host 崩溃、父进程退出、原会话归属、重复提交、宿主更新备份和拒绝条件。具体结果及 Linux/systemd 实机边界见 [VALIDATION](VALIDATION.md)，当前运维步骤见 [部署全流程](docs/DEPLOYMENT_LIFECYCLE.md)。
