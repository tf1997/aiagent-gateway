# 开发文档

按 [Hermes 适配与 A2A 1.0 开发方案](docs/HERMES_A2A_V1_DEVELOPMENT_PLAN.md)推进。P1/P2、A2A 1.0 和 Hermes HTTP 资源适配已实现，官方 SDK 六个 RPC 已通过两个真实 Hermes 实例的联调。第 7–12 节保留历史阶段记录；当前实现与验证进度见第 13 节。

## 1. 范围和约束

以 DESIGN.md 为架构依据，用 Go 1.24+ 标准库实现单节点网关。Route 直接内嵌 Endpoint，不引入独立服务池、数据库或 Redis。配置从 HTTP API 更新并原子落盘；会话、A2A context 和任务映射仅在内存，重启失效。

交付可运行的首版：独立管理/数据监听、Bearer 认证、路由和实例 CRUD、配置版本/ETag、创建幂等、原子文件恢复、HTTP/SSE 代理、加权调度、会话绑定、健康检查、排空和优雅停机。A2A 仅支持 1.0 JSON-RPC HTTP/SSE，方法为 SendMessage、SendStreamingMessage、GetTask、ListTasks、CancelTask、SubscribeToTask；不支持推送通知。规范固定到 https://github.com/a2aproject/A2A/tree/v1.0.1 。

## 2. 模块与开发顺序

| 文件 | 职责 |
| --- | --- |
| cmd/gateway/main.go | 环境变量、双端口启动、信号和停机 |
| internal/gateway/model.go | 配置、校验、错误与响应结构 |
| internal/gateway/store.go | 目录锁、原子落盘、不可变快照、幂等/审计 |
| internal/gateway/admin.go | 管理 CRUD、版本条件、分页与状态 |
| internal/gateway/runtime.go | 分片会话表、实例状态、调度、容量/TTL |
| internal/gateway/proxy.go | 认证、连接复用、HTTP/SSE 转发和超时 |
| internal/gateway/a2a.go | Card、协议验证、上下游标识映射 |
| api/openapi.yaml | 管理接口契约 |

先完成模型、存储与管理，再完成代理和绑定，然后接入 A2A、运行保障与网络集成测试。首版模块收敛在同一 internal 包内，避免早期接口抽象过多；后续按性能及维护需要拆包。

## 3. 一致性与并发

配置通过 atomic.Pointer 发布，只读 Map 和切片不可原地修改。管理更新持配置写锁和生命周期写锁，复制候选配置，检查引用，写同目录临时文件，fsync、rename、同步目录，最后发布。替换之前失败保留旧配置；替换之后同步失败进入不可用状态，重启校验恢复。目录使用操作系统独占文件锁，初版支持 Linux/macOS。

新绑定先持生命周期读锁，再加载快照、获取会话分片锁和路由调度锁，所有路径保持相同顺序。网络请求期间不持这些锁。运行状态由稳定的 routeID/endpointID 索引，配置更新不迁移有效绑定。A2A 标识映射须先完成再向客户端输出。

创建路由可一次提交多个实例；后续通过实例接口增删改。PATCH 采用 JSON merge patch 语义，路由 PATCH 不接受 endpoints；ID、版本不可改。更新和删除需 If-Match；创建支持 Idempotency-Key，同一身份/键对应不同请求返回冲突。审计与幂等记录有界。

## 4. 认证与运行配置

环境变量提供监听地址、数据目录、管理 Token 和调用方凭据，密钥不落入路由文件。调用方配置包含 ID、Token、可访问的 route key；身份隔离由服务器配置决定。上游凭据通过环境变量引用，代理移除客户端 Authorization、Cookie 和不可信身份头后注入可信身份/上游凭据。

首次运行必须显式初始化；已有文件不存在或损坏时不能隐式清空。ready 要求有效快照及持久化健康。管理和诊断端口默认仅绑定本机。

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

安装使用文件锁避免并发变更。先读入候选二进制和备份，再停止旧服务、原子替换二进制及 unit；启动失败恢复旧文件并尝试恢复原来的运行状态。首次创建独立管理及调用方凭据，升级不修改凭据和数据。普通 systemd 服务不设置 GATEWAY_INIT；安装时才调用一次性初始化 unit。启动检查通过 ExecStartPost 调用 --check-ready；主进程监听失败返回非零退出码，使 Restart=on-failure 生效。

`--init-only` 完成配置创建或验证后退出，不监听端口；`--check-ready` 根据配置的数据监听地址访问本机 /readyz，最长等待 15 秒，不打开配置存储锁。服务默认预留 30 秒停止时间，覆盖网关 20 秒优雅停机窗口。

控制流程测试使用临时文件系统和替代的命令执行器，验证首次安装、升级、回滚、状态丢失保护、卸载保留数据和嵌入资源；不调用真实 systemctl 或修改系统账户。真实 Linux 集成需验证 service 用户权限、EnvironmentFile 解析、重启/开机启动、端口冲突及进程异常后的重启行为。

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

`hermes_capabilities.go` 按接口缓存探测结果，Hermes 实例必须满足能力基线才能调度；管理接口同时展示健康、就绪和能力。`reconcile.go` 使用独立后台循环，有界轮转查询未完成/未知资源；查询失败不当成终态，stop 的 stopping 不提前释放执行归属。

真实 Hermes v2026.9.24 忽略 SendMessage 的 returnImmediately，且 SubscribeToTask 缺少标准要求的初始 Task。`hermes_a2a.go` 分别通过同实例流式入口的有界后台读取、同实例 GetTask 首帧补全实现公开 1.0 语义；新增并发释放、初始 Task、后台完成和停止生命周期测试。generic A2A 的严格标准检查保留。

本机增加第二个独立 Hermes 数据目录及端点；官方 a2a-sdk 固定 1.2.0，完整依赖锁定在 `deploy/hermes-local/requirements-a2a-sdk.txt`。SDK Card、六个 RPC、context 续接/分页、订阅取消已在两个实例上通过。原生资源、实例故障/排空和重启恢复边界由独立脚本检查，最终结果以 [验证记录](VALIDATION.md) 为准。P6 资源归属持久化仍按原方案单独交付。


## 14. 同机自动灰度升级（2026-10-06）

实现入口见 [操作说明](docs/ROLLOUT_OPERATIONS.md)，通用设计见 [HTTP 发布机制](docs/HTTP_GATEWAY_ROLLOUT_DESIGN.md)。新增固定入口、后台发布状态机、不可变 release 目录、独立 worker 端口和共享状态服务；默认 install 在新架构下验证真实代理结果后自动切流或回退。

单进程前台模式继续使用原内存绑定；灰度 worker 通过本机 RPC 共享路由/资源/幂等/并发额度。资源与会话日志由单写者提交，公共 ID 在持久提交后输出。普通业务升级不重启状态服务；状态代次变化会拒绝旧 worker 写入。短期列表快照跨 worker 共享，但不跨状态服务重启。

通用状态机和入口不依赖 Agent 包；systemd driver 及 Agent 适配位于本项目。安装与迁移仍需 Linux 实机验收，不以本机控制流程模拟或交叉构建作为已部署证明。
