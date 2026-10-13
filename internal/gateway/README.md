# Gateway 代码结构

根目录的 [gateway.go](gateway.go) 保留 `gateway.Open`、`gateway.Options`、共享状态服务及原有配置类型，供命令入口和进程配置包使用。

| 目录 | 职责 | 主要文件 |
| --- | --- | --- |
| [runtime](runtime/README.md) | 启动组装、调度、会话、健康与后台维护；配置、资源、共享通信和发布逻辑分别由子包负责 | `gateway.go`、`scheduler.go`、`sessions.go`、`health.go`、`background.go`；`config/`、`resource/`、`shared/`、`rollout/` |
| [a2a](a2a) | A2A 1.0 Card、请求/响应校验、资源映射、任务分页和 SSE；Hermes A2A 兼容处理 | `handler.go`、`codec.go`、`list.go`、`stream.go`、`hermes.go` |
| [hermes](hermes) | Hermes HTTP 路径与请求处理、会话分页、响应/事件观察和后台资源查询 | `handler.go`、`list.go`、`observe.go`、`stream.go`、`reconcile.go` |
| [protocol](protocol) | 共享模型、资源与缓存结构、适配器所需的运行时操作、JSON/HTTP/SSE 工具 | `model.go`、`resource.go`、`cache.go`、`backend.go`、`http.go` |

`gateway` 调用 `runtime`，后者组装 `a2a` 和 `hermes`；两个适配器只依赖 `protocol`，不互相导入，也不反向导入运行时。[runtime/protocols.go](runtime/protocols.go) 连接调度、资源、缓存和传输操作。锁、并发计数、存储和本地/共享状态切换由运行时维护，适配器接收资源副本和带释放函数的租约。

修改协议字段、路径或流处理时进入对应适配器。运行时内部的文件职责和依赖边界见 [Runtime 代码结构](runtime/README.md)。适配器与运行时共用的类型和操作放入 `protocol`，仅包内使用的编解码函数保持私有。

HTTP、A2A、Hermes 和跨 worker 的回归位于 [tests/integration](../../tests/integration/README.md)，通过 `gateway.Open`、公开 Handler、管理 API 和指标验证行为。需要访问锁、时钟、配置快照和故障注入点的测试保留在 `runtime/*_test.go`；独立资源测试随模块放在 `runtime/resource`。A2A 协议样例位于 [tests/integration/testdata](../../tests/integration/testdata)，旧配置样例保留在 [runtime/testdata](runtime/testdata)。

```sh
go test ./internal/gateway/... ./tests/integration
go test -race ./internal/gateway/... ./tests/integration
go vet ./...
go test ./internal/gateway/runtime -run '^$' -bench . -benchmem
```
