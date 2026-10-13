# Hermes Agent 会话接口与网关接入调研

> 历史调研说明：下文“当前网关”描述调研时的代码。2026-10-05 已实现 Hermes 专用 HTTP 适配与 A2A 1.0，并通过双实例原生资源和官方 SDK 联调，见 [验证记录](../VALIDATION.md) 与 [当前 HTTP 契约](HERMES_HTTP_API.md)。原始调研保留以追溯差异，不能作为当前网关说明。特别是固定 Hermes v2026.9.24 的 Runs events 不具备调研快照中的序号回放能力。


调研日期：2026-09-30。对象：[NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent)，不是同名模型或第三方 Hermes WebUI。

本文记录调研时的上游能力及本框架现状。后续改造按 [Hermes 适配与 A2A 1.0 开发方案](HERMES_A2A_V1_DEVELOPMENT_PLAN.md)推进；其中多入口路由和 A2A 1.0 支持现已实现。

## 1. 结论与版本范围

**Hermes 已经提供原生会话 REST API，不需要自行包装 CLI 才能对外提供 Agent 会话。** 启用 API Server 后，由 `hermes gateway` 在默认 `127.0.0.1:8642` 提供 `/api/sessions/*`、`/v1/chat/completions`、`/v1/responses` 和 `/v1/runs`。它们执行的是带工具、记忆和技能的 Hermes Agent。[H1] [H2]

对本框架，建议采用 `protocol: "proxy"` 接入 API Server：

- 需要创建、查询、管理会话：使用 **Sessions API**。
- 需要任务提交后独立查询、订阅、停止：使用 **Sessions API + Runs API**；上线可重试提交前，须先解决本框架删除 `Idempotency-Key` 的问题。
- 已有兼容 OpenAI 格式的客户端：按客户端能力选择 Chat Completions 或 Responses，明确由谁保存历史。
- **当前不能直接使用本框架的 `a2a` 模式接入所调研的 Hermes A2A 插件**：Hermes 使用 1.0 报文，本框架固定 0.3.0；旧方法名别名不代表报文兼容。

| 对象 | 本次固定版本 | 核查范围 |
| --- | --- | --- |
| Hermes 最新发布标签 | `v2026.9.24`，发布说明称 v0.21.5；提交 `f97608f178d1ffeca59860195ab7da295f7c8e5f` | 对照 API Server 文档、Sessions 路由注册和 A2A 协议定义 |
| Hermes 主分支快照 | `f42f579cf8bac4918ac9599bece71618afadd846`，2026-09-30 | 本文详细请求字段、事件、生命周期与源码分析的基准 |
| 本框架 | `f3227b54edf03b86919173d5ee79f6f79442cc0f` | HTTP/SSE 转发、绑定、认证头、健康检查及 A2A 实现 |

发布标签已包含本文所列原生 Sessions 路由。**详细行为以主分支快照为准，不把每项近期行为都视为发布版承诺。** 尚未取得实际部署的 Hermes 版本；接入时应先读取该实例的 `/v1/capabilities`。[H1] [H10]

## 2. 会话通过哪些入口暴露

| 入口 | 传输与会话标识 | 适用场景 | 本框架接入方式 |
| --- | --- | --- | --- |
| Sessions API | HTTP REST；路径中的 `session_id` | 会话 CRUD、历史、分支、同步/流式聊天 | `proxy` |
| Chat Completions | HTTP/POST，可 SSE；`messages`，可选 `X-Hermes-Session-Id` | 兼容现有聊天客户端 | `proxy` |
| Responses | HTTP/POST，可 SSE；`previous_response_id` 或 `conversation` | 服务端保存多轮响应链 | `proxy` |
| Runs API | POST 创建执行；GET 状态/SSE；`run_id` 关联 `session_id` | 长任务、重连、停止、人工审批、运行中补充指令 | `proxy`，需要额外保证 run 所属实例不变 |
| ACP | `hermes acp` 启动 stdio JSON-RPC 服务 | IDE/ACP host 集成 | 不能直接经现有 HTTP 代理，需要传输桥接 |
| Desktop/TUI/Dashboard | 另有 stdio/WebSocket JSON-RPC、Dashboard REST/终端通道 | Hermes 自有交互界面 | 不应与 API Server 的会话 REST 契约混用 |
| A2A 插件 | 独立 HTTP 入口，默认端口 9900；`contextId`、task ID | 跨 Agent 协议通信 | 需要 A2A 版本适配，见第 8 节 |

ACP 的标准输入输出、Dashboard 的登录/连接生命周期，与 API Server 的 Bearer REST 服务是不同接口。仅提供会话服务时，无需部署 Dashboard。[H6] [H7]

## 3. 如何开启 HTTP 会话服务

先完成 Hermes 的模型提供方与工具配置。在目标 profile 的 `.env` 中加入；默认路径为 `~/.hermes/.env`，命名 profile 为 `~/.hermes/profiles/<profile>/.env`：

```dotenv
API_SERVER_ENABLED=true
API_SERVER_HOST=127.0.0.1
API_SERVER_PORT=8642
API_SERVER_KEY=replace-with-hermes-upstream-secret
```

启动：

```sh
hermes gateway
```

原生会话服务与兼容接口共用该监听。`API_SERVER_ENABLED` 默认关闭；`API_SERVER_KEY` 在当前版本中必须配置，包括 loopback 部署。[H1]

下面命令假设调用 shell 中已经设置 `API_SERVER_KEY`。`.env` 被 Hermes 读取不等于变量自动出现在另一个 shell 中。

```sh
curl --fail-with-body http://127.0.0.1:8642/health

curl --fail-with-body http://127.0.0.1:8642/v1/capabilities \
  -H "Authorization: Bearer $API_SERVER_KEY"
```

优先核对 `features` 下的 `session_resources`、`session_chat`、`session_chat_streaming`、`session_fork`、`run_submission`、`run_status`、`run_events_sse`、`run_stop`，以及对应的 `endpoints`。需要审批/steer 时再检查相应能力。[H2] [H9]

跨容器或跨机器接入时，`127.0.0.1` 只对 Hermes 自己所在的网络命名空间有效，需将监听改为网关可达的地址。上游凭据应直接配置在路由接口的 `token` 中。

多 profile 有两种部署方式：独立端口，或启用 `gateway.multiplex_profiles` 后使用 `/p/<profile>/...`。后者必须携带目标 profile 自己的 `API_SERVER_KEY`；默认 profile 的 Key 不自动授权其他 profile。可以将 Endpoint URL 配为 `http://hermes-host:8642/p/alice`，由本框架拼接剩余路径。[H1]

## 4. 原生 Sessions REST 契约

以下为 **API Server** 的端点，不是本框架管理端 `/admin/v1/routes/{id}/sessions`。[H2]

| 方法 | 路径 | 请求/结果要点 |
| --- | --- | --- |
| GET | `/api/sessions` | 返回 `data` 列表；支持 `limit`、`offset`、`source`、`include_children`；默认 limit 50，上限 200 |
| POST | `/api/sessions` | 创建空会话；可传 `id` 或 `session_id`、`title`、`system_prompt`、模型选择等；成功 201 |
| GET | `/api/sessions/{id}` | 返回 `{"object":"hermes.session","session":{...}}`；未知 ID 返回 404 |
| PATCH | `/api/sessions/{id}` | 当前源码支持 `title`、`end_reason`、`pinned`、`archived`、`hidden`、`unread`；不是任意字段更新 |
| DELETE | `/api/sessions/{id}` | 删除历史及相关路由记录；存在活跃写保护时返回 409 `session_active_turn`，不能当作停止接口 |
| GET | `/api/sessions/{id}/messages` | 返回 `data`、解析后的 `session_id`、`pagination`；默认最近 500 条，可显式指定 `limit`/`offset`/`order` |
| POST | `/api/sessions/{id}/fork` | 返回新会话，复制历史并建立 lineage；当前实现还将父会话标记为 `branched` |
| POST | `/api/sessions/{id}/chat` | `{"input":"..."}` 或 `{"message":"..."}`；读取服务端历史，执行一轮，返回 assistant message、usage 等 |
| POST | `/api/sessions/{id}/chat/stream` | 相同输入，返回 Hermes 自定义 SSE 事件 |
| POST | `/api/sessions/{id}/model` | 持久化模型选择/锁定；属于需要按版本核对的扩展接口 |

### 4.1 创建时可以由客户端指定会话 ID

源码优先使用 `body.id`，其次 `body.session_id`；都没有时生成 `api_<时间戳>_<随机后缀>`。重复创建相同 ID 返回 **409 `session_exists`**，不是自动返回已有会话。[H2]

请求：

```http
POST /api/sessions
Authorization: Bearer <Hermes API_SERVER_KEY>
Content-Type: application/json

{"id":"client-generated-uuid","title":"项目排查"}
```

成功响应形状，省略其他元数据：

```json
{
  "object": "hermes.session",
  "session": {
    "id": "client-generated-uuid",
    "source": "api_server",
    "title": "项目排查"
  }
}
```

推荐由可信业务端生成 UUID，长度不超过 256，避免路径分隔符、控制字符及特殊路径片段。**显式创建后再聊天**便于区分“新建”与“恢复失败”，也能使网关从首次请求就绑定正确实例。

### 4.2 历史与会话 ID 可能存在 lineage

会话存储在目标 profile 的 `$HERMES_HOME/state.db`，包含 sessions、messages、工具调用以及压缩/分支关系；不是仅驻留某个 HTTP 连接的对象。[H5]

历史接口会解析恢复目标并包含压缩祖先。`include_compacted=true` 可以请求被压缩归档的内容。响应中的有效 `session_id` 可能与客户端原始 ID 不同，客户端应保存这一关系，同时保持网关路由键稳定。显式分支产生的新 ID 同样位于原实例：当前网关不会自动给它建立别名绑定。

对原生会话流，关注实际的 `run.completed` / `run.failed` / `run.cancelled` 事件；不能把出现 `assistant.completed` 或连接关闭本身当作业务执行成功。已读取的官方测试覆盖压缩祖先、分支标记、失败工具事件和中断后的状态投影。[H9]

## 5. 不同会话标识的职责

| 标识 | Hermes 中的用途 | 对本框架的含义 |
| --- | --- | --- |
| `/api/sessions/{id}` | 原生会话资源 ID | 当前网关不解析 URL 中的 ID，仍需请求头作为路由键 |
| `X-Hermes-Session-Id` | Chat Completions 的显式续接 ID；多个接口也在响应头中返回它 | 可配置为本框架 `session_policy.header`；单纯配置网关不会自动填充请求体/路径 |
| `X-Hermes-Session-Key` | 长期记忆/频道范围；部分接口也用它查找已声明会话 | 不是用户认证，不应无条件等同于一次聊天的 ID |
| `previous_response_id` | Responses 的上一轮响应链 ID | 当前网关不识别、不维护其归属 |
| `conversation` | Responses 命名会话，查找其最新 response | 当前网关不从正文提取它 |
| `run_id` | 一次执行及其事件、状态、审批、停止控制 | 不能直接替换原会话路由键，否则可能选到其他实例 |
| `X-Session-ID` | 本框架默认粘性头 | Hermes 没有因此自动续接原生会话的约定 |

### 5.1 Chat Completions 并非只有一种历史模式

不传 `X-Hermes-Session-Id` 时，历史来自请求 `messages`；Hermes 会根据 system prompt、首条 user message 等推导内部会话 ID。**这不等于调用方身份隔离，也不保证相同开场白对应不同会话。** 自有应用应显式生成 ID，不依赖内容指纹。[H3]

传该请求头时，Hermes 会尝试从 `state.db` 读取该会话的历史，正文提供当前输入；当前源码还会解析压缩后的有效会话，但响应仍回显客户端显式提供的稳定 ID。不要一边把完整正文历史当作权威，一边又启用服务端续接而未确认其优先级。[H3]

### 5.2 Responses 与原生 Sessions 不同

`POST /v1/responses` 使用 `previous_response_id` 或 `conversation` 保存/恢复响应链；两者不能同时传。`store` 默认 true，开启链式续接时应保持存储。未知 `previous_response_id` 返回 404。[H3]

**该处理器没有把请求头 `X-Hermes-Session-Id` 当作会话选择参数。** 即使本框架使用这个头做粘性，Hermes 的多轮续接仍由响应链等参数决定。`X-Hermes-Session-Key` 可参与已声明会话查找，但不应代替所选接口要求的历史管理契约。

Responses 快照保存在 profile 的 `response_store.db`；默认最多 100 个，按 LRU 淘汰，可跨正常重启保留。数据库打开失败时存在内存降级路径，因此“接口可调用”不等于持久化已正常工作。[H2]

## 6. Runs 与 SSE 的执行生命周期

### 6.1 Runs 将提交与订阅分开

```http
POST /v1/runs
Authorization: Bearer <Hermes API_SERVER_KEY>
Content-Type: application/json
Idempotency-Key: <每次业务提交唯一的键>

{"session_id":"client-generated-uuid","input":"分析当前项目"}
```

成功返回 HTTP **202**，JSON 包含 `run_id`、`status`、`replayed`。当 `session_id` 指向已有会话且没有显式历史/响应链参数时，Hermes 加载服务端历史。[H4]

官方 Runs 文档说明，同会话的写入由 session turn lease 串行协调，竞争后重新读取历史。本框架只保证绑定一致，不承担这种执行串行化；业务仍应明确同会话并发提交的顺序与超时处理。[H1]

| 方法 | 路径 | 行为 |
| --- | --- | --- |
| GET | `/v1/runs/{run_id}` | 查询运行状态和完成结果 |
| GET | `/v1/runs/{run_id}/events` | SSE 订阅，可独立于提交连接存在 |
| POST | `/v1/runs/{run_id}/stop` | 请求停止，先进入 `stopping`；执行实际退出后再进入终态 |
| POST | `/v1/runs/{run_id}/steer` | 向运行中的 Agent 补充指令，接受 `input`/`message`/`text` |
| POST | `/v1/runs/{run_id}/approval` | 响应 `approval.request`；按事件中的请求 ID 和允许选择提交，不能仅靠继续发聊天消息代替 |

所调研版本的 Runs 提供持久幂等：相同 Key、相同 JSON 和相同会话 Key 范围重试返回原 run；不同请求指纹返回 409 `idempotency_key_conflict`。Key 按 API profile/credential 隔离，官方文档给出的保留期为最后状态更新后 24 小时。部署时检查 `/v1/capabilities` 中 `features.runs_idempotency.durable`，不能仅看支持标志。[H1] [H4]

### 6.2 三种流的客户端处理不同

| 接口 | 主要事件格式 | 断开与重连 |
| --- | --- | --- |
| Chat Completions | `chat.completion.chunk`，另有命名事件 `hermes.tool.progress` | 不是 Runs 事件订阅；严格客户端应核对自定义事件支持 |
| 原生 `/chat/stream` | 命名 SSE：`assistant.delta`、`tool.started/completed/failed`、`approval.request`、`run.*` 等 | 断开后处理器会请求中断 live run，并保留控制引用直到执行退出；重新 POST 是新调用，不是续读 |
| Runs `/events` | SSE `id` 为序号，JSON `event` 标记 `message.delta`、`tool.*`、`run.*` 等 | 断开只移除订阅；使用 `Last-Event-ID` 或 `last_seq` 请求保留事件 |

API Server 的这些 SSE 路径在无事件约 10 秒时发送 `: keepalive` 注释。解析器应忽略注释，不能对每行直接做 JSON 解析。[H1] [H4]

原生 `/chat/stream` 也会产生 `run_id`，用于状态查询、停止和审批；但其事件队列没有注册到 Runs 的可重放缓存。需要独立的 `/events` 订阅/重连时，应从 `POST /v1/runs` 创建执行，不能仅拿原生聊天流里的 `run_id` 替换使用。[H2] [H4]

Runs 的重放是**有界内存事件缓存**：当前 backlog 最多 1000 项；没有订阅者的传输状态超过约 5 分钟会被回收；历史不足时会发出 `replay.truncated`。执行中的状态/控制引用不会仅因传输缓存过期而删除。普通终态状态有约 1 小时的内存保留；带持久幂等记录的 run 可从存储恢复状态，但这不恢复活跃执行或全部 SSE 历史。[H2] [H4]

Hermes 重启后，会话历史、响应链、幂等执行记录是不同存储层。不要把“历史还在”解释成“工具进程仍在原地继续”。停机中断的执行会进入 `interrupted` 等终态；客户端需查询后决定下一步。

## 7. 本框架可直接采用的接入配置

### 7.1 路由配置示例

以下是提交到本框架 `POST /admin/v1/routes` 的 JSON。`token` 填写对应 Hermes profile 的 `API_SERVER_KEY` 原值，不包含 `Bearer ` 前缀。

```json
{
  "key": "hermes",
  "protocol": "proxy",
  "session_policy": {
    "enabled": true,
    "header": "X-Hermes-Session-Id",
    "idle_ttl_seconds": 86400
  },
  "timeout": {
    "response_header_ms": 120000,
    "stream_idle_ms": 120000,
    "max_duration_ms": 3600000
  },
  "allowed_methods": ["GET", "POST", "PATCH", "DELETE"],
  "endpoints": [
    {
      "name": "hermes-1",
      "url": "http://127.0.0.1:8642",
      "health_url": "http://127.0.0.1:8642/health",
      "token": "replace-with-hermes-http-secret",
      "weight": 100
    }
  ]
}
```

此例先配置一个实例。超时数值是接入起点，不是所有任务的保证：同步调用若两分钟仍未发响应头仍会超时；长任务宜使用流或 Runs。网关原默认分别为 10 秒、120 秒、30 分钟。[G1] [G3]

`allowed_methods` 只限制 HTTP 方法，不限制上游路径。允许 POST 也会允许访问同一 API Server 中其他 POST 接口；需要端点级授权时应增加路径策略或在业务服务处约束。

### 7.2 经网关创建、续聊、读取历史

假设路由已创建，当前 shell 的 `GATEWAY_CALLER_TOKEN` 是有权访问 `hermes` 路由的调用方 Token。以下为待执行示例，本次调研没有向实际 Agent 提交任务。

```sh
HERMES_ROUTE_BASE='http://127.0.0.1:8080/proxy/hermes'
HERMES_ROUTING_SID="$(python3 -c 'import uuid; print(uuid.uuid4())')"

# 请求头供网关绑定；正文 id 创建同名 Hermes 会话。
curl --fail-with-body "$HERMES_ROUTE_BASE/api/sessions" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
  -H "X-Hermes-Session-Id: $HERMES_ROUTING_SID" \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$HERMES_ROUTING_SID\"}"

curl --fail-with-body -N \
  "$HERMES_ROUTE_BASE/api/sessions/$HERMES_ROUTING_SID/chat/stream" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
  -H "X-Hermes-Session-Id: $HERMES_ROUTING_SID" \
  -H 'Content-Type: application/json' \
  -d '{"input":"请说明你当前可用的工具，不执行外部操作。"}'

curl --fail-with-body \
  "$HERMES_ROUTE_BASE/api/sessions/$HERMES_ROUTING_SID/messages?limit=50&order=latest" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
  -H "X-Hermes-Session-Id: $HERMES_ROUTING_SID"
```

后续原生续聊保持同一请求头。若响应指向压缩后的 ID，可更新业务保存的 Hermes 资源 ID，但不要自动把网关路由键改成它。针对 Runs，正文传资源 `session_id`，查询/订阅/停止时继续使用原路由请求头，并保存 `run_id → 原路由键` 的关系。

若保留网关默认 `X-Session-ID` 也可接入；客户端需同时维护这个路由头和 Hermes 自己所需的 ID。配置自定义头只是减少一种标识，不会使网关理解 Hermes 的会话语义。[G1]

### 7.3 已确认的兼容点与缺口

| 项目 | 当前行为 | 接入要求 |
| --- | --- | --- |
| HTTP/SSE 路径 | 去掉 `/proxy/{routeKey}`，拼接 Endpoint URL；流式复制 | Endpoint 指向基础地址，避免重复 `/api`、`/v1`；保持 SSE 事件原样 |
| 上游认证 | 清除客户端 Authorization、Cookie，再注入 `Bearer <token>` | 网关 Token 与 Hermes Key 分开配置 |
| 会话绑定 | 仅内存；键为 caller ID、route ID、请求头 ID；默认空闲 24h | 重启/过期后多实例可能漂移；需要固定实例入口或可恢复的归属目录 |
| 未知资源 ID | 普通代理不会验证其原实例 | 对旧会话、response、run 不应随机重选；资源恢复失败由业务明确处理 |
| 幂等 | **删除 `Idempotency-Key` 和 `X-Idempotency-Key`** | Hermes Runs 的持久去重不会生效；须修改转发策略并联调后才能承诺安全重试 |
| 执行计数 | 网关计数到 HTTP 结束；Runs 返回 202 后仍可能执行 | 排空时同时检查 Hermes 在途任务；网关 `active=0` 不代表 Agent 空闲 |
| 调用方隔离 | 本框架注入 `X-Gateway-Caller-ID`；所核查 Hermes API 处理器没有据此做会话级授权 | 共享同一上游 Key/profile 不能获得自动的最终用户会话隔离；采用 profile 隔离或业务鉴权层 |
| Header/URL 一致性 | 网关不检查路由头与路径/正文会话 ID 是否对应 | 由可信业务层校验归属；不能把猜不到 UUID 当授权 |
| 会话列表 | 仅查询所选 Hermes 实例；网关管理 `/sessions` 仅列绑定 | 多实例需要单独索引/聚合；相同临时路由头也只能列一台 |
| 健康检查 | 只按上游 HTTP 状态判断，且不注入 Endpoint 凭据 | 可用 `/health` 做存活检查；不能直接把需鉴权且降级仍返回 200 的 `/health/detailed` 当就绪判断 |
| 并发限制 | 网关默认每实例 HTTP 并发 256；Hermes API 默认最大并发执行 10，超限 429 | 按实际 Agent 容量协调限流；HTTP 并发与后台执行数不是同一个指标 |
| WebSocket/浏览器 | 当前代理定位 HTTP/SSE；没有浏览器跨域预检专门处理 | Desktop/终端 WS 需要适配；浏览器直接使用 SSE 时还要解决 Bearer、自定义头和 CORS |

以上网关结论来自当前实现，相关代码见 [G1]、[G2]、[G3]。尤其是 `proxy.go` 的响应体统一包装只提供 `io.ReadCloser`，不能仅因使用了 Go `ReverseProxy` 就认为现有包装链已支持双向 WebSocket 升级。

浏览器原生 `EventSource` 不支持这里的 POST 聊天接口，也不能任意设置 Authorization/路由头；可使用支持流式读取的 fetch 或服务端中转。Hermes 的 CORS 配置不会绕过本框架先行的 Bearer 认证；无 Token 的 OPTIONS 预检当前会被网关拒绝。

## 8. A2A：存在服务端能力，但与本框架版本不兼容

Hermes 的 A2A 插件可通过 `hermes gateway setup` 选择启用，默认端口 9900；提供 `GET /.well-known/agent-card.json`、`POST /` JSON-RPC 和 SSE。会话按 A2A `contextId` 组织。这是另一种暴露方式，不需要把 Sessions REST 包装成 A2A。[H8]

本次核查发现以下实际差异：

| 对比项 | Hermes 当前 A2A | 本框架 |
| --- | --- | --- |
| 声明版本 | `PROTOCOL_VERSION = "1.0"`，发布标签中也如此 | Agent Card 固定 `protocolVersion: "0.3.0"` |
| 方法 | `SendMessage`、`SendStreamingMessage`、`GetTask`、`SubscribeToTask` 等；部分旧方法名仍接受 | 只接受 `message/send`、`message/stream`、`tasks/get`、`tasks/cancel`、`tasks/resubscribe` |
| Task/Message 结构 | 1.0 通过成员区分结构，task 不携带旧 `kind`；新方法还带 `task`/`message` 外层 | `mapResult` 强制要求 `kind` 为 `task`/`message`/事件种类 |
| 状态/角色 | `TASK_STATE_COMPLETED`、`ROLE_USER` 等 | 终态判断使用 `completed` 等，消息角色检查 `user`/`agent` |

即使发送 Hermes 接受的旧别名 `message/send`，返回的 task 仍缺少本框架所需 `kind`，会在 `mapResult` 被拒绝为 `unsupported result kind`。Hermes 为该别名省略 1.0 外层包装，并没有转换整套 0.3.0 数据结构。**改路由配置或只改方法名不足以兼容。** [H8] [G4]

后续若必须走 A2A，应单独实现并测试 1.0 ↔ 0.3.0 的消息、任务、状态、流事件、取消和重订阅转换，或者升级本框架的协议支持。使用普通 `proxy` 原样转发 1.0 是另一条技术路径，但不会获得本框架现有 A2A ID 映射能力，Agent Card 的对外地址和多实例归属也需另行处理。

## 9. 接入验收建议与本次验证边界

建议按以下顺序做真实联调：

1. **能力与基本会话**：读取实际版本/capabilities；显式 ID 创建、重复创建、续聊、历史、未知 ID、删除活跃会话。
2. **粘性与 lineage**：至少两个 Hermes 实例；检查创建/聊天/历史/run 控制是否命中原实例，测试压缩、fork、网关 TTL 和重启。
3. **流与取消**：长工具调用、心跳、客户端断开；分别验证原生 chat stream 与 Runs events 的不同行为。
4. **Runs 幂等与恢复**：先修正本框架请求头转发；测试相同 Key 重放、不同正文冲突、Hermes 重启后的状态恢复、`Last-Event-ID` 与事件缓存淘汰。
5. **权限与排空**：跨 caller/profile 访问、错误 Key、路径/请求头不一致；Hermes 后台 run 未结束时不得仅按网关活跃 HTTP 计数删除实例。

本次完成的是官方文档、固定提交源码、发布标签路由对照，以及本框架实现的静态核查；阅读了官方 Sessions 测试，**没有运行 Hermes、调用真实模型/工具、执行官方测试或进行双实例端到端验证**。示例供后续联调使用，不表示已验证部署成功。

## 10. 依据

源码链接固定到本次提交，避免 `main` 更新后改变结论；产品文档可从 [Hermes 官方文档](https://hermes-agent.nousresearch.com/)查阅更新。

- [H1] API Server 官方文档：开启方式、接口说明、配置、SSE、Runs、认证与 profile。
- [H2] API Server 源码：路由表、Sessions 处理器、ResponseStore、SSE 生命周期、默认 TTL。
- [H3] 兼容接口处理器：Chat Completions/Responses 的 ID 与历史优先级。
- [H4] Runs 源码：提交、状态、事件重放、停止、审批和幂等存储调用。
- [H5] 会话存储文档：profile、SQLite、历史与 lineage。
- [H6] ACP 官方文档：stdio 暴露方式。
- [H7] TUI transport：stdio/WebSocket 传输区分。
- [H8] A2A 官方协议实现；另见 [A2A 接入文档][H8D]、[方法别名与处理器][H8A]。
- [H9] 官方 Sessions 测试：能力发现、历史、分支、中断与事件。
- [H10] 最新发布说明；对应 [发布标签路由源码][H10S]。
- [G1] 本框架 HTTP/SSE 代理、认证与请求头处理。
- [G2] 本框架会话绑定、TTL、实例调度与健康检查。
- [G3] 本框架默认会话策略、超时与字段校验。
- [G4] 本框架 A2A 版本、方法、报文与终态判断。

[H1]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/website/docs/user-guide/features/api-server.md
[H2]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/gateway/platforms/api_server.py#L1745
[H3]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/gateway/platforms/api_server_openai_routes.py#L636
[H4]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/gateway/platforms/api_server_runs.py
[H5]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/website/docs/developer-guide/session-storage.md
[H6]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/website/docs/user-guide/features/acp.md
[H7]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/tui_gateway/transport.py
[H8]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/plugins/platforms/a2a/protocol.py#L22
[H8A]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/plugins/platforms/a2a/adapter.py#L46
[H8D]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/website/docs/user-guide/messaging/a2a.md
[H9]: https://github.com/NousResearch/hermes-agent/blob/f42f579cf8bac4918ac9599bece71618afadd846/tests/gateway/test_session_api.py
[H10]: https://github.com/NousResearch/hermes-agent/releases/tag/v2026.9.24
[H10S]: https://github.com/NousResearch/hermes-agent/blob/f97608f178d1ffeca59860195ab7da295f7c8e5f/gateway/platforms/api_server.py#L1587
[G1]: ../internal/gateway/proxy.go
[G2]: ../internal/gateway/runtime.go
[G3]: ../internal/gateway/model.go
[G4]: ../internal/gateway/a2a.go
