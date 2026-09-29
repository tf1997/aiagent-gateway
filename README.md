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
