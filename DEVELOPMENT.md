# 开发文档

## 1. 范围和约束

以 DESIGN.md 为架构依据，用 Go 1.24+ 标准库实现单节点网关。Route 直接内嵌 Endpoint，不引入独立服务池、数据库或 Redis。配置从 HTTP API 更新并原子落盘；会话、A2A context 和任务映射仅在内存，重启失效。

交付可运行的首版：独立管理/数据监听、Bearer 认证、路由和实例 CRUD、配置版本/ETag、创建幂等、原子文件恢复、HTTP/SSE 代理、加权调度、会话绑定、健康检查、排空和优雅停机。A2A 固定 0.3.0 JSON-RPC HTTP 绑定，支持 message/send、message/stream、tasks/get、tasks/cancel、tasks/resubscribe；不支持推送通知。规范：https://a2a-protocol.org/v0.3.0/specification/。

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
