# AI Agent Gateway

Go 单节点 Agent 网关：HTTP 管理路由、一个路由内嵌多个实例、会话粘性、HTTP/SSE 代理，以及 A2A 0.3.0 JSON-RPC 转发。无第三方 Go 依赖。

设计见 [DESIGN.md](DESIGN.md)，开发约定见 [DEVELOPMENT.md](DEVELOPMENT.md)，管理接口契约见 [api/openapi.yaml](api/openapi.yaml)。

## 快速运行

需要 Go 1.24+，支持 Linux/macOS。先启动自己的 Agent 服务，再执行：

```sh
export GATEWAY_ADMIN_TOKEN='replace-with-admin-secret'
export GATEWAY_CALLERS='[{"id":"app-a","token":"replace-with-caller-secret","routes":["research"]}]'
export GATEWAY_DATA_DIR='./data'
export GATEWAY_PUBLIC_URL='http://localhost:8080'
GATEWAY_INIT=true go run ./cmd/gateway
```

`GATEWAY_INIT=true` 仅允许在没有配置文件时初始化。常规重启移除该变量；已有文件损坏时始终拒绝启动。数据面默认 `:8080`，管理面默认 `127.0.0.1:9090`，可通过 `GATEWAY_LISTEN` / `GATEWAY_ADMIN_LISTEN` 修改。部署时持久化数据目录，TLS 由入口代理终止。

创建一个普通代理路由：

```sh
curl http://localhost:9090/admin/v1/routes \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: research-v1' \
  -d '{
    "key":"research",
    "protocol":"proxy",
    "endpoints":[
      {"name":"instance-1","url":"http://127.0.0.1:8001","weight":100},
      {"name":"instance-2","url":"http://127.0.0.1:8002","weight":100}
    ]
  }'
```

路由默认启用会话粘性，同一调用方使用相同会话 ID 会命中原实例：

```sh
curl -N http://localhost:8080/proxy/research/chat \
  -H 'Authorization: Bearer replace-with-caller-secret' \
  -H 'X-Session-ID: session-001' \
  -H 'Content-Type: application/json' \
  -d '{"message":"hello"}'
```

若上游无需会话粘性，创建路由时传 `"session_policy":{"enabled":false}`。上游 URL 的路径作为前缀，查询参数合并保留。凭据通过 Endpoint 的 `credential_ref` 指定环境变量名，启动或修改配置时校验变量存在；客户端 Authorization、Cookie 和身份转发头会被清理。

## Linux systemd 一键安装

服务管理已内置在二进制中，无需安装脚本。将编译好的 Linux 二进制复制到服务器，执行：

```sh
sudo ./aiagent-gateway install
```

从源码在 Linux 上构建并安装：

```sh
go build -o bin/aiagent-gateway ./cmd/gateway
sudo ./bin/aiagent-gateway install
```

在 macOS 等开发机交叉构建 Linux 版本（ARM64 服务器将 `amd64` 改为 `arm64`）：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/aiagent-gateway-linux-amd64 ./cmd/gateway
```

将产物复制到 Linux 服务器后执行 `sudo ./aiagent-gateway-linux-amd64 install`。

目标服务器需要运行 systemd，并提供 `systemctl`、`getent`、`groupadd` 和 `useradd`。安装需要 root；运行服务使用独立的 `aiagent-gateway` 系统用户。二进制内嵌两个 unit 文件，目标机器无需 Go、源码或额外脚本。

安装命令完成以下工作：

- 安装当前二进制到 `/usr/local/bin/aiagent-gateway`。
- 首次安装生成独立随机管理 Token 和调用方 Token，保存于 `/etc/aiagent-gateway/gateway.env`，权限 0600；Token 不输出到终端。
- 首次通过一次性初始化服务创建 `/var/lib/aiagent-gateway/config.json`；常规启动不自动创建缺失的配置文件。
- 启动服务，通过 `/readyz` 检查后启用开机自启动；异常退出由 systemd 自动重启。
- 重复执行 `install` 更新二进制和 unit，保留凭据和路由数据。升级会重启进程，内存会话和 A2A 映射会清空；升级启动失败时尝试恢复旧二进制、unit 和原来的运行状态。

默认数据面为 `:8080`，管理面为 `127.0.0.1:9090`。首次生成的调用方 ID 为 `default`，可访问所有路由。通过 root 编辑配置调整调用方权限、上游凭据和 `GATEWAY_PUBLIC_URL`；后者应改为客户端实际可访问的地址。配置文件使用 systemd `EnvironmentFile` 语法，例如 JSON 值用单引号包围，文件由 systemd 读取，不作为 shell 脚本执行。

```sh
sudo aiagent-gateway status
sudo aiagent-gateway restart
sudo aiagent-gateway stop
sudo aiagent-gateway start
sudo journalctl -u aiagent-gateway -f
```

停止服务不会取消下次开机启动；若要禁用开机启动，执行 `sudo systemctl disable aiagent-gateway`。卸载服务及已安装二进制：

```sh
sudo aiagent-gateway uninstall
```

卸载保留凭据、路由数据和系统用户，后续安装可复用。已有安装缺少凭据或配置文件时，`install` 拒绝覆盖，需先恢复相应文件。

macOS 可前台运行网关，但不支持这些 systemd 管理命令。当前已完成安装控制流程的模拟测试和 Linux 交叉构建，真实 systemd 启停/开机自启需要在 Linux 主机复核。

## 管理与持久化

- `POST/GET /admin/v1/routes`：创建、分页查询路由。
- `GET/PATCH/DELETE /admin/v1/routes/{id}`：详情、策略修改、删除。
- `/admin/v1/routes/{id}/endpoints`：POST 添加、GET 分页查询实例。
- `/admin/v1/routes/{id}/endpoints/{endpointID}`：GET、PATCH、DELETE。
- `/admin/v1/routes/{id}/sessions`、`/tasks`：运行时绑定查询。
- `/admin/v1/config/status`：内存版本及持久化健康状态。
- `/admin/v1/audit-logs`、`/metrics`：审计、Prometheus 文本指标，均需管理 Token。

分页参数为 `offset`、`limit`（默认和最大均为 100）。详情 GET 返回 ETag，PATCH/DELETE 必须使用该资源的 `If-Match`；不匹配返回 412。创建支持 `Idempotency-Key`，相同键和相同原始请求重放结果，不同请求返回 409。首版保存最近 1000 条幂等记录与 1000 条审计。

配置写入同目录临时文件，经 fsync、rename、目录同步后发布内存快照。文件和目录权限默认 0600 / 0700。多个进程不能同时打开同一数据目录；文件替换后目录同步失败时 ready 失败，需排查存储并重启恢复。

实例状态支持 active、draining、disabled。draining 不接新会话，已有会话继续访问。有绑定或活跃调用的实例不能删除或替换地址。实例 `health_url` 可指定主动健康检查地址，默认每 10 秒检查，不假定上游提供 `/healthz`。

## A2A

创建路由时将 `protocol` 设为 `a2a`，实例 URL 指向各自 JSON-RPC 入口，另传 `agent_card`：

```json
{
  "name":"Research Agent",
  "description":"Research assistant",
  "version":"1.0.0",
  "capabilities":{"streaming":true},
  "skills":[],
  "defaultInputModes":["text/plain"],
  "defaultOutputModes":["text/plain"]
}
```

Card 位于 `/agents/{routeKey}/.well-known/agent-card.json`，需要调用方认证；网关改写地址、认证声明和协议版本。设置 `GATEWAY_PUBLIC_URL` 为客户端实际可访问的网关基础地址。

支持 `message/send`、`message/stream`、`tasks/get`、`tasks/cancel`、`tasks/resubscribe`。任务和 context 使用网关生成的 ID，隔离不同调用方及实例的同名 ID；协议数据中的文件、parts 和 metadata 保持原样。已有任务与 context 必须携带网关返回的标识；未知任务返回协议错误，不随机转发。

不支持 REST/gRPC 绑定、推送通知、扩展 Card、任务编排或普通 HTTP Agent 到 A2A 的通用转换。调用方可在首次请求通过 `X-Session-ID` 提供稳定会话键。A2A 任务和 context 映射只存内存：重启后旧标识不可恢复，非终态任务保留映射，终态任务与无任务引用的 context 空闲 24 小时后清理。

## 性能与运行边界

路由快照原子读取、会话索引使用 256 个分片、已有会话按实例 ID 查找；新会话按权重轮询。HTTP 连接和复制缓冲复用，默认没有应用层重试。普通代理请求体上限 32 MiB；管理/A2A JSON 和单个 A2A SSE 事件上限 2 MiB。SSE 使用独立的空闲/最大时长控制，客户端断开会取消上游 HTTP 请求。

默认全局并发 4096、每调用方 1024、每实例 256，会话和任务容量各 100000。超限返回不可用，不排无限队列。上限可通过 `GATEWAY_MAX_INFLIGHT`、`GATEWAY_MAX_PER_CALLER`、`GATEWAY_MAX_PER_ENDPOINT`、`GATEWAY_MAX_SESSIONS`、`GATEWAY_MAX_TASKS` 调整。会话空闲 TTL 默认 24 小时，活跃请求不会被过期清理。

首版提供 Prometheus 基础计数和进程启动/停机结构化日志；OpenTelemetry、延迟直方图、被动熔断和完整访问日志尚未接入。主动健康检查串行执行，大规模实例部署前需要压测并改为有界并发。A2A 映射采用独立互斥锁，A2A 高并发场景需进一步压测。设计文档中的全部生产运行保障不等于已经实现。

## 验证

```sh
go test ./...
go test -race ./...
go vet ./...
go test ./internal/gateway -run '^$' -bench . -benchmem
go build -o bin/gateway ./cmd/gateway
```

受限环境可用 `GATEWAY_TEST_IN_MEMORY=true go test -race ./...`：测试通过 `net.Pipe` 跑实际 net/http 编解码、流式转发与取消，不监听 TCP 端口。当前已验证此模式；真实 TCP 测试、真实 A2A SDK/Agent 互操作和目标机器容量压测仍待执行，微基准不能代替生产吞吐指标。
