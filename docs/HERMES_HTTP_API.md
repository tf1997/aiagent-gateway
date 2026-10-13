# Hermes HTTP 资源适配

路由设置 `agent_type: "hermes"`、`expose: ["proxy", "a2a"]` 后，同一 `/agents/{appId}` 同时提供原生 Hermes HTTP 和 A2A 1.0。不同智能体/profile 使用不同路由和端点；同一路由的多个端点是同一智能体的副本。

## 会话与调用方

```http
POST /agents/assistant/api/sessions
Authorization: Bearer <gateway caller token>
Content-Type: application/json

{"id":"my-conversation","title":"项目讨论"}
```

创建返回 `session.id`，形如 `session_...`。客户端保存此公开 ID，随后调用 `/api/sessions/{id}`、`/messages`、`/chat` 或 `/chat/stream`。创建时的 `id` / `session_id` 是该调用方的别名，不会直接发送给 Hermes。不同调用方可以使用相同别名，实际公开 ID、上游 ID 和实例归属分别维护。

同一调用方重复创建相同别名返回 409。未知或其他调用方的资源返回 404；不能用上游 ID 绕过目录。路径、`session_id` 正文字段、`X-Hermes-Session-Id` 和路由的会话头同时出现时必须指向同一会话，否则拒绝。`session_policy.enabled=false` 只关闭 generic 粘性，不关闭 Hermes 的资源归属检查。

已支持的原生路径：

| 方法 | 路径 | 行为 |
| --- | --- | --- |
| POST / GET | `/api/sessions` | 创建 / 当前调用方的会话列表 |
| GET / PATCH / DELETE | `/api/sessions/{id}` | 详情 / 元数据 / 删除 |
| GET | `/api/sessions/{id}/messages` | 历史，保留 limit、offset、order 等上游查询参数 |
| POST | `/api/sessions/{id}/fork` | 创建新会话，固定在父会话实例 |
| POST | `/api/sessions/{id}/chat` | 同步聊天 |
| POST | `/api/sessions/{id}/chat/stream` | 原生 SSE 聊天 |
| POST | `/api/sessions/{id}/model` | 模型选择/锁定 |
| GET | `/v1/models`、`/v1/capabilities` | 模型与经过网关限制后的能力 |

会话压缩产生的新上游 ID 会作为同一公开会话的别名登记；旧入口继续访问原实例。fork 使用独立公开 ID。DELETE 不代替停止正在运行的任务；上游的 409 等错误状态保留。删除成功的公开会话不再用于新请求，相关映射按资源保留规则回收。

`X-Hermes-Session-Key` 在发往上游前按调用方、路由进行哈希命名；省略时使用当前公开会话的独立作用域。该处理隔离 API 关联标识，不隔离共享 profile 内的工具权限、文件和长期记忆存储。

## 会话列表

列表仅包含该调用方经当前网关登记的会话，不转发上游全量列表。网关按归属逐一查询原实例；任一实例发生非 404 查询失败时，整个列表失败，不返回伪装为完整结果的残缺列表。

```http
GET /agents/assistant/api/sessions?limit=50&include_children=true
```

响应包含 `data`、`limit`、`offset`、`total`、`has_more` 和 `next_page_token`。下一页传入 `page_token`，并保留相同的 limit、source、include_children 参数。游标绑定调用方、路由版本和筛选条件，快照有效期 2 分钟。

默认 limit 50，上限 200；最多 1000 个候选会话、8 MiB 快照、4 MiB 单页。每个进程最多同时构建 4 份列表，保留 64 份快照 / 32 MiB。超限明确拒绝，调用方应缩小结果集。上游直接创建和网关重启后丢失归属的会话不自动导入。

## Runs 与幂等

```http
POST /agents/assistant/v1/runs
Authorization: Bearer <gateway caller token>
Content-Type: application/json
Idempotency-Key: operation-unique-key

{"session_id":"session_...","input":"分析这份材料"}
```

Runs 必须引用已创建的公开会话。返回的 `run_id` 是网关 ID，随后可调用：

- `GET /v1/runs/{run_id}` 查询状态。
- `GET /v1/runs/{run_id}/events` 订阅 SSE；网关保留 `Last-Event-ID` 或 `last_seq`，能否重放取决于上游版本。
- `POST /v1/runs/{run_id}/stop` 请求停止。
- `POST /v1/runs/{run_id}/steer` 补充输入。
- `POST /v1/runs/{run_id}/approval` 提交审批决定，使用 Hermes 事件中的 request ID 与选项。

`Idempotency-Key` 仅在 Runs 提交中支持。网关在发送前预留原实例和请求指纹，向上游发送包含调用方、路由及操作范围的哈希键；同键同请求重试固定原实例，同键不同请求返回 409。上游实际去重取决于其 `runs_idempotency.supported` 能力，低版本可能不支持。其他写接口带此头会被拒绝，不宣称它们具有相同去重保证。上游 5xx 或网络错误不触发自动重发或重新调度。

固定发布版 `v2026.9.24` 的 Runs 事件是一次性队列，没有事件序号，完成订阅后再次访问 events 返回 404。调研主分支中的重放缓存不属于此发布版；网关不为它虚构重放能力，公开 `run_events_replay` 为 false。支持重放的上游仍可收到原始 Last-Event-ID，并保留其序号和 replay.truncated 提示。

停止响应可能只是 `stopping`；202、SSE 结束、订阅断开或 HTTP 并发归零均不代表执行已完成。网关保留 Run 归属及父会话引用，通过状态接口和后台核查确认终态。原生 chat stream 里的 run 可以查询/停止/审批，但 Hermes 不保证其具有 `/events` 的重放缓存。

## Chat Completions 与 Responses

`POST /v1/chat/completions` 支持同步和 SSE，必须通过 `X-Hermes-Session-Id`（或等价的已验证会话引用）指向已创建的会话，不依赖开场白指纹选择会话。

`POST /v1/responses` 支持同步、SSE、`previous_response_id` 和字符串形式的 `conversation`。两种续接参数互斥；新响应链创建独立归属，旧链固定原实例。命名 conversation 也按调用方隔离。请求和响应中的标准标识转换为对应公开/上游 ID，不递归修改文本、工具输入输出或 metadata。上游 store / 缓存淘汰限制仍然适用。

## 流与能力发现

Hermes 请求 JSON 上限 2 MiB，普通响应 4 MiB，单个 SSE 帧 2 MiB。SSE 按事件校验和转换后立即刷新，保留心跳注释、事件名称、`id`、`retry` 以及 Runs 重放提示。未闭合帧、无终态的提前 EOF、非法标识或超时会报告中断，不伪造完成状态。

Hermes 路由按接口缓存能力，30 秒内复用探测结果。HTTP 探测 `/v1/capabilities`，A2A 探测接口基础地址下的 `/.well-known/agent-card.json`。能力探测用于诊断和能力展示，不作为调度门槛：低版本缺少部分能力、探测接口缺失或失败、缓存缺失或过期，都不会使健康接口拒绝代理。新请求选择 active 且目标接口健康的实例，已有资源继续固定原实例；具体操作是否支持由上游实际响应确定。配置的 `expose`、网关支持的路径和 A2A 协议版本仍限定可用入口。

管理 Endpoint 详情中的 `runtime.interfaces` 提供 `healthy`、`ready`、`http_inflight` 和 `capabilities`。接口 `ready` 反映健康状态；`capabilities.ready` 仅表示最近一次探测是否满足能力基线，为 false 不阻止代理。HTTP 不健康不会直接把 A2A 标记为不健康。

后台每 10 秒轮转检查最多 16 个符合条件的资源，每次查询最多 3 秒，独立于健康检查运行。查询失败保留未知状态和原归属；未获得任何上游 ID 的不确定提交不能凭空核查，客户端应使用原幂等键恢复或人工排查。保留期和内存容量沿用公共目录规则。

## Hermes A2A 发布版差异

官方 `v2026.9.24` 有两个需要适配的行为：

1. `SendMessage` 忽略 `returnImmediately`。Hermes 路由在此场景调用同一实例的流式入口，先返回首个 Task，再继续读取有界后台流；客户端收到确认后关闭连接不会取消该流，显式取消仍用 CancelTask。
2. `SubscribeToTask` 不发送初始 Task。网关先从已绑定实例 GetTask，再将该 Task 作为公开订阅的首帧，随后转换上游更新。

这些处理只作用于 Hermes 路由。公开接口仍为 A2A 1.0；generic A2A 保持严格的标准流检查，不恢复 0.3 兼容。

## 恢复边界

配置持久化，资源归属、别名、幂等预留和列表快照仍在内存。网关重启后旧公开资源返回不可恢复/不存在错误，不能重新调度到其他实例。上游自身的持久化不等于网关可以恢复映射，因此对外 `runs_idempotency.durable` 为 false。跨重启恢复属于开发方案中的 P6，未纳入本期 P1–P5。

本机双实例部署与联调命令见 [部署说明](../deploy/hermes-local/README.md)，实际执行结果见 [验证记录](../VALIDATION.md)。
