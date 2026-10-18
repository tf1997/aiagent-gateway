# Runtime 代码结构

`runtime` 组装网关模块，协调调度、会话绑定、生命周期锁和本地/共享状态切换。对外入口仍由上层 [gateway.go](../gateway.go) 提供。

| 子包 | 职责 | 主要文件 |
| --- | --- | --- |
| [config](config) | 管理 HTTP API、路由校验与快照、消费者认证和脱敏、旧配置迁移及原子存储 | `admin.go`、`views.go`、`upsert.go`、`route.go`、`consumers.go`、`migration.go`、`store.go` |
| [resource](resource) | 资源目录、别名与父子关系、幂等预留、生命周期、归属恢复；封装资源锁 | `directory.go`、`reservation.go`、`observation.go`、`hermes.go`、`lifecycle.go`、`durable.go`、`ownership.go` |
| [shared](shared) | Unix Socket 客户端和服务端、操作重放与代次校验、worker 租约、列表快照缓存 | `wire.go`、`client.go`、`server.go`、`cache.go` |
| [rollout](rollout) | Agent 请求的灰度归属解析、worker 身份和控制接口、HTTP/SSE 结果观察 | `resolve.go`、`worker.go`、`result.go`、`transport.go` |

根目录保留需要同时协调多个模块的逻辑：

| 文件 | 职责 |
| --- | --- |
| [gateway.go](gateway.go) | `Options`、启动与关闭、目录锁和模块初始化 |
| [scheduler.go](scheduler.go) | 加权调度、并发额度和执行租约 |
| [sessions.go](sessions.go) | generic 会话分片、worker 归属、会话日志与恢复 |
| [health.go](health.go)、[background.go](background.go) | 实例健康与能力发现、后台资源核查、过期回收和缓存清理 |
| [proxy.go](proxy.go)、[protocols.go](protocols.go) | HTTP 入口与传输、A2A/Hermes 适配器组装、资源调度与租约转换 |
| [admin.go](admin.go) | 配置事务与生命周期锁、持久化后发布快照、运行指标 |
| [shared.go](shared.go)、[rollout.go](rollout.go) | 共享领域操作分发和发布模块接线 |

子包不反向导入 `runtime`。`config.Backend`、`shared.Backend` 和 `rollout` 的操作接口由运行时提供；资源模块通过共享客户端切换到状态服务，调用方拿到资源副本或释放函数，不接触目录内部 Map 和锁。适配器继续只依赖上层 [protocol](../protocol) 契约。

配置更新仍在配置锁和生命周期写锁内完成，持久化成功后原子替换路由 Map。磁盘存储拆为版本元数据 `config.json`、路由 `routes.json` 和管理幂等/审计 `route-logs.json`，通过 `.config.pending.json` 恢复跨文件写入。资源锁顺序、共享请求字段和共享状态日志格式沿用原有语义。

运行参数 `Options` 在 `gateway.go` 中定义，上层 `gateway.Options` 继续作为对外别名。运行时直接引用 `protocol.Route`、`protocol.Config` 等共享模型，原 `options.go` 中的中转别名已移除。

根目录的 7 个测试文件保留需要访问私有状态的用例：`admin_test.go` 验证持久化故障和快照不可变性，`scheduler_test.go` 验证调度、绑定与并发额度，`background_test.go` 验证撤销认证后的后台核查，`protocols_test.go` 验证缓存时钟和额度，`resources_test.go`、`shared_test.go` 验证资源与共享状态内部不变量，`helpers_test.go` 提供包内辅助函数。独立资源测试仍随 `resource` 子包存放。

HTTP、A2A、Hermes、Token 轮换和跨 worker 请求的 53 个集成测试位于 [tests/integration](../../../tests/integration/README.md)，使用公开入口、管理查询和指标验证行为。A2A 协议 fixtures 随集成测试迁移；旧配置迁移 fixture 保留在 [testdata](testdata)。

```sh
go test ./internal/gateway/... ./tests/integration
go test -race ./internal/gateway/... ./tests/integration
go vet ./...
go test ./internal/gateway/runtime -run '^$' -bench . -benchmem
```
