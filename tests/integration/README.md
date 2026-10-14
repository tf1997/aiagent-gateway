# 网关集成测试

本目录使用独立的 `integration_test` 包，通过 `gateway.Open`、`AdminHandler`、`DataHandler`、`WorkerHandler` 和共享状态公开入口验证组件协作。路由、资源和并发状态从管理 API、`/metrics` 或实际请求结果读取。

| 文件 | 覆盖范围 |
| --- | --- |
| `gateway_test.go` | 存储初始化与恢复、HTTP 路径、流式输出、取消和超时 |
| `admin_test.go` | 路由 upsert、ETag、幂等回执、公开配置模型 |
| `upstream_errors_test.go` | HTTP/A2A/Hermes 错误详情、列表及异步错误、读取超时、entry/worker 转发 |
| `consumers_test.go`、`credentials_test.go` | 消费者轮换与撤销、上游 Token 转发、持久化与脱敏 |
| `interfaces_test.go` | 统一入口、兼容路径、接口禁用与能力声明 |
| `a2a_test.go`、`a2a_list_test.go` | A2A 1.0 请求/响应、资源隔离、分页、SSE 和取消 |
| `hermes_test.go` | 会话谱系、Run 重放、后台核查、流式生命周期与能力探测 |
| `shared_test.go` | state/entry/worker 协作、切流、跨 worker 资源与分页 |
| `helpers_test.go` | 管理查询、请求构造和模拟上游辅助函数 |

所有上游均由测试启动，使用临时目录、本地 TCP 随机端口和 Unix Socket。无需运行中的网关、真实 Agent 或凭据。`GATEWAY_TEST_IN_MEMORY` 仅控制 runtime 包内测试，本目录始终使用本地网络。后台核查测试会等待真实维护循环，整组测试通常需要十几秒。

在仓库根目录运行：

```sh
go test ./tests/integration
go test -race ./internal/gateway/... ./tests/integration
go test ./...
```

需要操纵锁、配置快照、容量计数或缓存时钟的测试保留在 [runtime](../../internal/gateway/runtime/README.md)；独立资源测试随 [resource](../../internal/gateway/runtime/resource) 包存放。这里创建 A2A 任务时经过 `SendMessage`，创建 Hermes 资源时经过对应 HTTP API。

A2A 固定规范和 JSON 样例位于 [testdata/a2a-v1.0.1](testdata/a2a-v1.0.1/README.md)。本地模拟测试与真实 Hermes/官方 SDK 联调分别记录在 [VALIDATION.md](../../VALIDATION.md)。
