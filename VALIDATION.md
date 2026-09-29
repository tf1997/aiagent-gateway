# 首版验证记录

日期：2026-09-29。环境：macOS arm64、Apple M3、Go 1.24.4。

## 已完成

| 检查 | 结果 |
| --- | --- |
| `GATEWAY_TEST_IN_MEMORY=true go test -race -cover ./...` | 通过；核心包语句覆盖率 60.6%，启动入口未计入集成覆盖 |
| `go vet ./...` | 通过 |
| `go build -o bin/gateway ./cmd/gateway` | macOS arm64 构建通过 |
| `GOOS=linux GOARCH=amd64 go build ... ./cmd/gateway` | Linux amd64 交叉构建通过，未在 Linux 运行 |
| 管理 OpenAPI | JSON/YAML 语法与本地 schema 引用检查通过 |

自动测试覆盖：配置创建/修改/恢复、创建幂等及冲突、ETag、目录独占锁、损坏文件、替换前后持久化失败、同会话并发绑定、调用方隔离、并发上限释放、排空及引用保护、故障不重选、TTL/容量、代理状态码/路径/查询参数、凭据清理、SSE 首事件及时输出、断开取消、上游空闲超时、A2A JSON 与 SSE 标识映射、任务权限、会话冲突和重启丢弃任务映射。

HTTP 集成测试采用 net.Pipe 内存双向连接，运行真实 net/http 服务和客户端编解码，不使用 TCP 监听。未设置 `GATEWAY_TEST_IN_MEMORY` 时，同一测试套件使用真实 loopback TCP。

## 微基准

`go test ./internal/gateway -run '^$' -bench BenchmarkBoundSession -benchmem -benchtime=1s`，8 路并发竞争同一已绑定会话，一次操作包含 acquire/release：

```text
BenchmarkBoundSession-8   2517038   459.9 ns/op   122 B/op   5 allocs/op
```

这不是代理吞吐或生产容量结果；未包含认证、HTTP、TLS、正文复制、A2A JSON 映射或上游延迟。优化方向包括减少 lease/闭包分配、状态索引锁竞争以及 A2A 映射锁竞争，需用实际负载的 pprof 进一步判断。

## 待外部环境验证

- 真实 TCP 测试：沙箱禁止临时端口监听。自动审批服务返回 404（所选审核模型不可用），没有执行提升权限的测试命令；这不是对测试安全性的否定。
- 真实 A2A 0.3.0 SDK/Agent 互操作、任务取消和订阅恢复的完整端到端场景。
- 目标机器 HTTP/SSE 压测：吞吐、p95/p99、连接容量、长期内存稳定性、慢客户端与停机。
- 文件崩溃窗口目前通过注入返回错误检查行为；真实磁盘满、进程 kill 和掉电恢复未验证。

在可监听端口的开发机上执行 `go test -race ./...` 进行 TCP 复核。生产部署前补齐上述验证，不以首版自动测试作为生产性能承诺。


## 内置 systemd 管理验证

新增服务管理使用二进制子命令和内嵌 unit，无外部安装脚本。

- `GATEWAY_TEST_IN_MEMORY=true go test -race -cover ./...` 通过；网关核心包覆盖率 60.6%；服务管理包另补充首次未加载 unit 和系统账户校验回归测试。
- 服务管理控制流程覆盖首次安装、升级保留凭据和数据、启动失败回滚、重试安装、缺失数据拒绝重建、卸载保留数据、凭据权限及符号链接保护。测试使用临时目录与模拟 systemctl/account 命令。
- 二进制 CLI 冒烟检查通过：`--init-only` 创建/重复验证、损坏配置拒绝覆盖、帮助输出、未知子命令退出码、监听错误非零退出、非法 readiness 地址拒绝。
- `go vet ./...` 和 `git diff --check` 通过。
- Linux amd64 和 arm64 静态交叉构建通过，产物位于 `bin/aiagent-gateway-linux-amd64` 和 `bin/aiagent-gateway-linux-arm64`（构建目录不提交 Git）。

当前主机为 macOS，未修改任何真实 systemd 服务或系统账户。Linux 上仍需验证 unit 加载、EnvironmentFile 解析、服务用户权限、实际启动/重启、开机自启和端口占用失败行为；模拟测试与交叉构建不等于这些环境验证。
