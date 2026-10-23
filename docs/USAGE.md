# 网关使用说明

本文面向网关部署者和应用接入方，按当前代码说明启动、路由配置、HTTP/SSE、Hermes、A2A 调用及日常运维。命令默认在项目根目录执行，示例地址和 Token 请替换为实际值。

## 1. 先了解三个概念

| 概念 | 用途 | 示例 |
| --- | --- | --- |
| Route（路由） | 一个对外提供服务的 Agent；`key` 用在客户端 URL 中 | `research`、`assistant` |
| Endpoint（实例） | 同一 Agent 的一个运行副本；一个路由可包含多个实例 | `http://127.0.0.1:8001` |
| Caller（调用方） | 在路由 consumers 中登记的应用身份，Token 可动态更换 | `app-a` |

调用链：应用 → 网关 `/agents/{key}/...` → 该路由下的 Agent 实例。网关不负责启动 Agent 或配置模型，需先准备可访问的上游服务。同一路由的多个实例应是同一 Agent 的副本；不同 Agent 或 Hermes profile 分别建路由。

网关有两类端口、两类 Token：

| 入口 | 本机访问示例 | 认证 | 用途 |
| --- | --- | --- | --- |
| 数据接口 | `http://127.0.0.1:8080` | 调用方 Token | HTTP/SSE、Hermes、A2A |
| 管理接口 | `http://127.0.0.1:9090` | 管理 Token | 路由、实例、运行状态、指标 |

数据监听默认为 `:8080`，管理监听默认为 `:9090`，均监听所有网卡，支持通过网关 IP 或域名远程访问。`/healthz` 和 `/readyz` 位于数据端口，无需 Token。对外 HTTPS 由入口反向代理提供；SSE 接入时应关闭代理响应缓冲，并设置足够长的读取超时。

管理 Token、各调用方 Token 必须不同。客户端 Token 只用于网关认证，上游凭据由网关单独配置。

## 2. 本地启动

需要 Go 1.24+，支持 Linux/macOS。本文示例使用 `curl` 和 `jq`，后者用于提取响应 ID。

在终端 A 中复制示例并填写配置：

```sh
cp config.example.yaml config.yaml
chmod 600 config.yaml
```

编辑 `config.yaml`，例如：

```yaml
port: 8080
admin_port: 9090
data_dir: "./data"
public_url: "http://127.0.0.1:8080"
admin_token: "replace-with-admin-secret"
```

`port` 和 `admin_port` 只填端口号；数据面和公共管理面均监听所有网卡。

编译并启动：

```sh
go build -o bin/aiagent-gateway ./cmd/gateway
./bin/aiagent-gateway --config ./config.yaml
```

进程在前台运行，日志输出到终端，按 `Ctrl+C` 停止。首次启动和重启使用同一条命令：路由存储缺失时自动初始化，已有文件直接读取，损坏时拒绝启动。省略 `--config` 默认读取当前目录的 `config.yaml`；相对路径以配置文件所在目录为准。

旧 `GATEWAY_*` 启动环境变量和上游凭据环境变量不再读取。完整字段和迁移步骤见 [YAML 配置说明](CONFIGURATION.md)。

在终端 B 设置后续请求使用的变量：

```sh
GATEWAY_URL='http://127.0.0.1:8080'
GATEWAY_ADMIN_URL='http://127.0.0.1:9090'
GATEWAY_ADMIN_TOKEN='replace-with-admin-secret'
GATEWAY_CALLER_TOKEN='replace-with-caller-secret'

curl -fsS "$GATEWAY_URL/healthz"
curl -fsS "$GATEWAY_URL/readyz"
```

正常返回 `{"ok":true}` 和 `{"ready":true}`。就绪检查只说明网关能够接收请求，上游是否可用需通过实际业务请求验证。`GATEWAY_CALLER_TOKEN` 是本文 curl 示例使用的客户端变量；服务端通过路由 `consumers` 登记和校验消费者，不读取 YAML callers 或本节客户端变量。

从其他机器调用时，将 `GATEWAY_URL` 和 `GATEWAY_ADMIN_URL` 中的 `127.0.0.1` 替换为网关可达的 IP 或域名，并使用配置的端口，例如 `GATEWAY_ADMIN_URL='http://gateway.example.com:9090'`；经过 HTTPS 入口代理时使用对应的 HTTPS 地址。后续管理请求使用该地址，并携带 `Authorization: Bearer $GATEWAY_ADMIN_TOKEN`。

## 3. 创建普通 HTTP 路由并调用

假设两个 Agent 副本已分别监听 `8001`、`8002`，且均提供 `POST /chat`。以下请求将它们注册到 `research`：

```sh
curl -fsS "$GATEWAY_ADMIN_URL/admin/v1/routes" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: create-research-v1' \
  -d '{
    "key":"research",
    "consumers":[{"id":"app-a","token":"replace-with-caller-secret"}],
    "agent_type":"generic",
    "expose":["proxy"],
    "timeout":{
      "response_header_ms":180000,
      "stream_idle_ms":120000,
      "max_duration_ms":1800000
    },
    "endpoints":[
      {"name":"instance-1","interfaces":{"proxy":{"url":"http://127.0.0.1:8001"}},"weight":100},
      {"name":"instance-2","interfaces":{"proxy":{"url":"http://127.0.0.1:8002"}},"weight":100}
    ]
  }' | tee /tmp/gateway-research-route.json

ROUTE_KEY='research'
ENDPOINT_ID=$(jq -r '.endpoints[0].id' /tmp/gateway-research-route.json)
```

只部署一个实例时删掉第二个 Endpoint。key 不存在时创建，响应 HTTP 201；key 已存在时更新，响应 HTTP 200。响应包含内部 `id`（形如 `rt_...`）、`key`、实例 ID（`ep_...`）和版本号。**管理接口与业务 URL 都可以直接使用路由 key**；旧管理接口仍兼容内部路由 ID，路径优先匹配 key。

同一 `Idempotency-Key` 重发完全相同的 POST 请求会返回原状态码及结果；更改正文或 URL 后复用该键返回 409。新的更新操作需使用新幂等键或不传该头。管理端保留最近 1000 条 POST 幂等记录；重试时保留原始正文，包括空白格式。

### HTTP 和 SSE 调用

```sh
curl -fsS -N "$GATEWAY_URL/agents/research/chat" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
  -H 'X-Session-ID: session-001' \
  -H 'Content-Type: application/json' \
  -d '{"message":"你好"}'
```

这里的 `/chat`、正文和响应格式由上游定义。`generic` 透传业务协议；上游返回 SSE 时网关流式转发，`curl -N` 关闭客户端输出缓冲。上游使用 `stream:true` 等开关时，按其接口要求传入。

路径转换示例：

| 上游 `interfaces.proxy.url` | 客户端请求 | 实际上游请求 |
| --- | --- | --- |
| `http://127.0.0.1:8001` | `/agents/research/chat` | `http://127.0.0.1:8001/chat` |
| `http://127.0.0.1:8001/api` | `/agents/research/chat` | `http://127.0.0.1:8001/api/chat` |
| `http://127.0.0.1:8001` | `/agents/research/v1/chat/completions` | `http://127.0.0.1:8001/v1/chat/completions` |

上游 URL 的路径是前缀，查询参数会合并。兼容聊天客户端的 base URL 可设置为 `$GATEWAY_URL/agents/research/v1`，并提供调用方 Token；默认启用粘性时还需传会话头。避免同时在上游 URL 和客户端路径重复添加 `/v1`。

### 会话粘性与负载分配

路由默认启用粘性，普通 HTTP 请求必须携带 `X-Session-ID`，缺失返回 `400 SESSION_ID_REQUIRED`。同一调用方、路由和接口内，相同会话 ID 继续访问原实例；新会话按实例权重轮询。修改权重或扩容只影响新分配，不迁移已有绑定。

普通粘性绑定默认空闲 24 小时后过期，过期后再次使用相同 ID 会重新选实例。原实例故障或被禁用时，已有绑定请求失败，不自动转到另一实例。网关不在应用层自动重试失败请求。

无状态服务可在创建路由时加入以下配置，免传会话头：

```json
{"session_policy":{"enabled":false}}
```

### 上游需要认证时

在对应实例的接口配置中填写独立的上游 Token，通过路由管理 API 提交：

```json
{
  "proxy": {
    "url": "http://127.0.0.1:8001",
    "token": "replace-with-upstream-secret",
    "health_url": "http://127.0.0.1:8001/health"
  }
}
```

该对象是 `interfaces` 的值。`health_url` 可省略；配置后约每 10 秒检查一次，携带该接口凭据，仅 2xx 视为健康，不跟随重定向。只填写上游实际存在的探活路径。

`token` 保存该接口的上游 Token，网关注入 `Authorization: Bearer <token>`；不要包含 `Bearer ` 前缀。路由更新即时生效，省略保留原值，`null` 或空字符串清除认证。Token 随路由持久化，管理响应不回显；YAML 和环境变量不提供上游凭据。

网关清理客户端的 Authorization、Cookie 和不可信身份转发头，并注入 `X-Gateway-Caller-ID`。普通 `generic` 代理还会移除 `Idempotency-Key` / `X-Idempotency-Key`，不提供业务请求去重保证。

## 4. 接入 Hermes

Hermes 路由使用 `agent_type:"hermes"`，网关管理 session/run/response 的归属和公开 ID。以下示例使用 HTTP 地址 `http://127.0.0.1:18642`；如上游有认证，请将下例 `token` 替换为实际的 Hermes Token。

```sh
curl -fsS "$GATEWAY_ADMIN_URL/admin/v1/routes" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: create-assistant-v1' \
  -d '{
    "key":"assistant",
    "consumers":[{"id":"app-a","token":"replace-with-caller-secret"}],
    "agent_type":"hermes",
    "expose":["proxy"],
    "timeout":{"response_header_ms":180000,"stream_idle_ms":120000,"max_duration_ms":1800000},
    "endpoints":[{
      "name":"hermes-1",
      "interfaces":{"proxy":{"url":"http://127.0.0.1:18642","token":"replace-with-hermes-http-secret"}}
    }]
  }'
```

上游无认证时删除 `token`。Hermes 会探测 `/v1/capabilities` 并展示能力信息；能力不足或探测失败不阻止健康实例代理，具体操作是否支持由上游响应确定。

### 先创建会话，再发送消息

```sh
curl -fsS "$GATEWAY_URL/agents/assistant/api/sessions" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"id":"my-conversation-001","title":"项目讨论"}' \
  | tee /tmp/gateway-hermes-session.json

SESSION_ID=$(jq -r '.session.id' /tmp/gateway-hermes-session.json)

curl -fsS -N "$GATEWAY_URL/agents/assistant/api/sessions/$SESSION_ID/chat/stream" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"input":"请简要介绍一下你能做什么"}'
```

创建时的 `id` 是调用方自己的别名，后续使用响应中的 **`session.id` 公开 ID**。同一调用方重复创建相同别名返回 409。同步聊天将 `/chat/stream` 改为 `/chat`；历史消息使用 `GET /api/sessions/$SESSION_ID/messages`。

兼容 Chat Completions 的调用也要引用已创建会话：

```sh
curl -fsS -N "$GATEWAY_URL/agents/assistant/v1/chat/completions" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
  -H "X-Hermes-Session-Id: $SESSION_ID" \
  -H 'Content-Type: application/json' \
  -d '{"model":"hermes-agent","messages":[{"role":"user","content":"继续刚才的话题"}],"stream":true}'
```

路径、正文及会话头同时引用资源时必须一致。Hermes 不能靠关闭 `session_policy.enabled` 绕过资源归属检查。

### 提交后台 Run

```sh
jq -n --arg sid "$SESSION_ID" '{session_id:$sid,input:"分析当前项目的改进方向"}' \
  | curl -fsS "$GATEWAY_URL/agents/assistant/v1/runs" \
      -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
      -H 'Content-Type: application/json' \
      -H 'Idempotency-Key: analysis-operation-001' \
      --data-binary @- \
  | tee /tmp/gateway-hermes-run.json

RUN_ID=$(jq -r '.run_id' /tmp/gateway-hermes-run.json)

curl -fsS "$GATEWAY_URL/agents/assistant/v1/runs/$RUN_ID" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN"

curl -fsS -N "$GATEWAY_URL/agents/assistant/v1/runs/$RUN_ID/events" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN"
```

Run 提交支持 `Idempotency-Key`：同键同请求固定原实例，由上游配合去重；同键不同请求返回 409。Hermes 其他写接口不接受此头。停止用 `POST /v1/runs/{run_id}/stop`，返回 `stopping` 或 202 仍需继续查询终态。

`/v1/responses`、会话 fork、模型设置、steer、审批和列表分页见 [Hermes HTTP 接口说明](HERMES_HTTP_API.md)。不支持的 jobs、文件、浏览器及系统管理路径返回 501。固定 Hermes `v2026.9.24` 的 Run events 是一次性订阅，不支持断线后事件重放。

已有本仓库本机 Hermes 环境时，使用 [本机部署说明](../deploy/hermes-local/README.md) 中的脚本；该环境默认网关端口为 **18080 / 19090**，路由为 `hermes-local`，调用方凭据在其 `.runtime/gateway/client.env` 中，与本文手动启动示例不同。

## 5. 接入 A2A 1.0

网关支持 A2A 1.0 JSON-RPC/SSE。先准备符合该版本的 A2A 上游，路由配置示例如下；提交方式同第 3 节，以下使用独立 key `research-a2a`，在路由正文的 `consumers` 中配置消费者即可，无需修改 YAML 或重启。

```json
{
  "key": "research-a2a",
  "consumers": [{"id":"app-a","token":"replace-with-caller-secret"}],
  "agent_type": "generic",
  "expose": ["a2a"],
  "timeout": {"response_header_ms":180000,"stream_idle_ms":120000,"max_duration_ms":1800000},
  "agent_card": {
    "name": "Research Agent",
    "description": "Research assistant",
    "version": "1.0.0",
    "capabilities": {"streaming":true},
    "skills": [],
    "defaultInputModes": ["text/plain"],
    "defaultOutputModes": ["text/plain"]
  },
  "endpoints": [{
    "name": "instance-1",
    "interfaces": {
      "a2a": {"url":"http://127.0.0.1:9900/","protocol_version":"1.0"}
    }
  }]
}
```

`interfaces.a2a.url` 是上游准确的 JSON-RPC 地址，包含其路径。需要上游认证时增加 `token`。只有上游支持流式时才声明 `streaming:true`；省略整个 `agent_card` 会生成默认 Card，但默认不声明 streaming。

同一 Agent 同时提供 HTTP 和 A2A 时，将 `expose` 设为 `["proxy","a2a"]`，并在**每个** Endpoint 的 `interfaces` 中同时配置两项，两个接口须属于同一个 Agent 副本。Hermes 上游设为 `agent_type:"hermes"`；网关不会把普通 HTTP 服务自动转成 A2A。

读取 Card：

```sh
curl -fsS "$GATEWAY_URL/agents/research-a2a/.well-known/agent-card.json" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN"
```

Card 中的 RPC 地址由 YAML `public_url` 生成，应设置为客户端实际可访问的网关基础地址。

发送消息：

```sh
curl -fsS "$GATEWAY_URL/agents/research-a2a/a2a" \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'A2A-Version: 1.0' \
  -d '{
    "jsonrpc":"2.0",
    "id":1,
    "method":"SendMessage",
    "params":{
      "message":{"messageId":"m-001","role":"ROLE_USER","parts":[{"text":"你好"}]},
      "configuration":{"returnImmediately":true}
    }
  }' | tee /tmp/gateway-a2a-result.json
```

结果可能是 `result.task` 或 `result.message`。若返回 Task，保存其公开 `id` 和 `contextId`，随后查询：

```sh
TASK_ID=$(jq -er '.result.task.id' /tmp/gateway-a2a-result.json)
jq -n --arg id "$TASK_ID" '{jsonrpc:"2.0",id:2,method:"GetTask",params:{id:$id}}' \
  | curl -fsS "$GATEWAY_URL/agents/research-a2a/a2a" \
      -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
      -H 'Content-Type: application/json' \
      -H 'A2A-Version: 1.0' \
      --data-binary @-
```

| 方法 | 用途 |
| --- | --- |
| `SendMessage` | 发送消息；默认等待完成或等待输入/认证，`returnImmediately:true` 可提前返回任务 |
| `SendStreamingMessage` | 流式发送；使用 SSE 客户端或 `curl -N` |
| `GetTask` | 查询公开 task ID |
| `ListTasks` | 查询当前调用方在该路由登记的任务 |
| `CancelTask` | 取消任务 |
| `SubscribeToTask` | 订阅已有任务的 SSE 更新 |

所有 RPC 必须带 `A2A-Version: 1.0`，不兼容旧 `message/send` 等方法名。后续请求使用网关返回的 task/context ID；不能替换为上游 ID。HTTP 与 A2A 的粘性绑定分别维护，相同会话字符串不保证跨协议续接。

JSON-RPC 协议错误可能使用 **HTTP 200 + `error` 对象**，客户端除检查 HTTP 状态外还要检查正文。缺少版本头或版本不支持返回 `error.code:-32009`。`ListTasks` 默认 `pageSize:50`、上限 100，按 `nextPageToken` 翻页，游标有效期 2 分钟。

上游 A2A 错误保留 `error.code`、`error.message` 和可选 `error.data`，包括 SSE 错误事件。若上游返回非 JSON-RPC 的 HTTP 错误，详情放在 `error.data.upstream` 中，包含 HTTP `status` 和 `body`；连接失败的具体原因放在 `error.data.upstream.message`。

## 6. 管理路由、扩容与下线

管理请求均携带管理 Token。列表格式为 `{"items":[...],"total":...,"offset":...,"limit":...}`，`limit` 默认及上限均为 100。

```sh
curl -fsS "$GATEWAY_ADMIN_URL/admin/v1/routes?offset=0&limit=100" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN"
```

下文沿用第 3 节的 `ROUTE_KEY=research` 和响应中的 `ENDPOINT_ID`。若换了终端，设置路由 key 后，通过 `GET /admin/v1/routes/research` 取得实例 ID。

### 按 key 查询和更新路由

| 操作 | 请求 | 说明 |
| --- | --- | --- |
| 创建或更新 | `POST /admin/v1/routes` | 正文必填 key；不存在时创建（201），存在时更新（200） |
| 查询详情 | `GET /admin/v1/routes/{key}` | 返回路由、实例和 ETag |
| 查询列表 | `GET /admin/v1/routes` | offset/limit 分页 |
| 删除 | `DELETE /admin/v1/routes/{key}` | 无引用时删除，有引用返回 409 |

例如修改现有路由的超时，不需要先取内部 ID 或 ETag：

```sh
curl -fsS "$GATEWAY_ADMIN_URL/admin/v1/routes" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"key":"research","timeout":{"response_header_ms":180000}}'

curl -fsS "$GATEWAY_ADMIN_URL/admin/v1/routes/research" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN"
```

更新只合并提交的路由字段；未传 `endpoints` 时保留实例列表，传入时按完整列表替换。各实例项是完整配置，省略的 weight/state 按创建默认值处理；建议每项使用稳定且唯一的 `name`，同名实例保留内部 ID 和会话归属。未命名实例仅按完全相同的 interfaces 匹配，不能通过数组顺序识别实例；匹配歧义或重复名称返回 409。`endpoints:[]` 表示移除全部实例，`endpoints:null` 不合法。

需要同时增加 A2A 时，在一次 POST 中提交相同 key、`expose:["proxy","a2a"]`，以及每个实例同时包含 proxy/a2a 的完整列表。无需先删除路由重建；添加接口保留原实例 ID，已有聊天绑定仍有效。

POST 更新和按 key 删除默认无需 `If-Match`，并发提交以最后一次成功提交为准。需要乐观锁时带上路由最新 ETag；过期返回 412，对不存在的 key 携带 If-Match 也返回 412。首次创建需提供 `expose` 等完整必要字段，单独提交 key 和超时不能创建新路由。`id`、`version` 是只读字段，不要把 GET 响应原样作为 POST 正文。

完整网关的 install 同时发布管理接口与业务代码；前台模式重启为新二进制即可。旧实验拓扑需人工维护切换，安装器不自动迁移。

### 动态配置消费者

消费者不在 YAML 中配置。创建路由时可一起提交 `consumers`，也可对已有路由单独更新：

```sh
curl -fsS "$GATEWAY_ADMIN_URL/admin/v1/routes" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"key":"research","consumers":[{"id":"app-a","token":"new-caller-secret"}]}'
```

成功保存后，新 HTTP、Hermes 和 A2A 请求立即按新 Token 验证，入口和业务执行器同步使用最新配置，无需重启。消费者 ID 是资源归属标识：轮换 Token 保持 ID，原会话、任务和实例绑定继续有效；修改 ID 视为另一个消费者，不能继承旧资源。同名 ID 跨路由共享每调用方并发额度，资源仍按路由隔离。

| consumers 输入 | 行为 |
| --- | --- |
| 省略字段 | 更新时保留；创建时无消费者，业务请求全部拒绝 |
| `[{"id":"app-a","token":"new-secret"}]` | 替换完整列表；创建新消费者或轮换同 ID 的 Token |
| `[{"id":"app-a"},{"id":"app-b","token":"another-secret"}]` | 保留 app-a 的原 Token，增加 app-b；未列出的消费者被撤销 |
| `[]` | 撤销该路由所有消费者 |

每路由最多 1000 个消费者，ID 与 Token 在该路由内不能重复。新消费者必须传 Token，已有消费者省略 token 保留原值；空值/null、只读 token_hash 和与管理 Token 相同的值均被拒绝。Token 建议使用足够长的随机字符串。

配置文件仅保存消费者 Token 的 SHA-256 哈希；创建/更新/GET/列表和幂等回执只返回消费者 ID，不返回 Token 或哈希。客户端自己保存原 Token。消费者撤销会拒绝之后的新请求，已经接收的连接/后台任务不会被强制取消，后台继续核查已有执行状态。灰度发布期间路由写入仍按原规则冻结。

### 添加实例

先部署可用的新副本，再提交：

```sh
curl -fsS "$GATEWAY_ADMIN_URL/admin/v1/routes/$ROUTE_KEY/endpoints" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: research-add-instance-3' \
  -d '{"name":"instance-3","interfaces":{"proxy":{"url":"http://127.0.0.1:8003"}},"weight":100}'
```

实例必须提供该路由开放的全部接口。权重范围 1–10000，默认 100；两个健康 active 实例权重为 100 和 200 时，新分配大致按 1:2 分布。权重不能设为 0，需要停止接新会话时使用 draining。

### 修改或排空实例

单独修改/删除实例仍需携带实例详情 GET 返回的最新 ETag，包括双引号。以下先读实例版本，再设置排空：

```sh
curl -fsS -D /tmp/gateway-endpoint.headers \
  "$GATEWAY_ADMIN_URL/admin/v1/routes/$ROUTE_KEY/endpoints/$ENDPOINT_ID" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -o /tmp/gateway-endpoint.json

ETAG=$(awk 'tolower($1)=="etag:" {gsub("\r", "", $2); print $2}' /tmp/gateway-endpoint.headers)

curl -fsS -X PATCH \
  "$GATEWAY_ADMIN_URL/admin/v1/routes/$ROUTE_KEY/endpoints/$ENDPOINT_ID" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -H "If-Match: $ETAG" \
  -H 'Content-Type: application/json' \
  -d '{"state":"draining"}'
```

| 实例状态 | 新会话/新归属 | 已有会话/资源请求 |
| --- | --- | --- |
| `active` | 可接收，仍受健康与容量限制 | 可继续 |
| `draining` | 不再分配新会话；Hermes fork 也被拒绝 | 可继续，包括已有会话的新 Run/response |
| `disabled` | 不接收 | 拒绝，不迁移到其他实例 |

查询实例详情的 `runtime.http_inflight`、`runtime.resource_references` 和 `runtime.resources`，确认连接及资源引用释放后再删除。HTTP 并发归零不代表后台任务结束；普通粘性绑定也可能继续保留到空闲 TTL。仍有绑定或引用时，删除或替换地址返回 409。

删除前重新 GET 实例取得 ETag，然后执行：

```sh
curl -fsS -X DELETE \
  "$GATEWAY_ADMIN_URL/admin/v1/routes/$ROUTE_KEY/endpoints/$ENDPOINT_ID" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -H "If-Match: $ETAG"
```

每次修改后 ETag 都可能变化，不要复用 PATCH 前的值。变更权重可将 PATCH 正文改为 `{"weight":200}`，恢复接收新会话则为 `{"state":"active"}`。

也可使用兼容的 `PATCH /admin/v1/routes/{key}` 修改路由策略，但仍需提供路由本身的 ETag，且不接受 `endpoints`。推荐使用本节的 POST 按 key 更新。

### 删除路由

确认不再需要路由、且会话和任务引用已释放后，按 key 删除：

```sh
curl -fsS -X DELETE "$GATEWAY_ADMIN_URL/admin/v1/routes/research" \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN"
```

未知 key 返回 404，有绑定或资源引用返回 409。按内部 ID 删除的兼容路径仍要求 If-Match。

### 查看会话及资源归属

| 管理路径 | 内容 |
| --- | --- |
| `/admin/v1/routes/{key}/sessions` | 普通粘性绑定，支持 `endpoint_id` 筛选 |
| `/admin/v1/routes/{key}/tasks` | A2A 任务 |
| `/admin/v1/routes/{key}/resources` | Hermes session/run/response、A2A context/task |
| `/admin/v1/routes/{key}/runs` | Hermes Run |
| `/admin/v1/config/status` | 已应用版本、持久化版本和存储健康 |
| `/admin/v1/audit-logs` | 管理操作审计 |
| `/metrics` | Prometheus 文本指标，仍需管理 Token |

`resources` / `runs` 支持 `owner_id`、`endpoint_id`、`interface` 筛选；`resources` 还支持 `resource_kind`。完整请求字段见 [管理接口契约](../api/openapi.yaml)。

## 7. 配置速查

### YAML 进程配置

监听、数据目录、管理认证和容量统一写入 `config.yaml`；消费者和上游 Token 通过路由管理 API 配置；字段默认值见 [完整配置表](CONFIGURATION.md#字段与默认值)，可直接复制 [config.example.yaml](../config.example.yaml)。原环境变量已弃用且不会回退读取。

容量值须为正整数。YAML 修改后重启加载；路由和实例继续通过管理 API 修改，成功保存后生效。

### 路由默认超时和大小限制

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `timeout.response_header_ms` | 10000（10 秒） | 等待上游响应头 |
| `timeout.stream_idle_ms` | 120000（2 分钟） | 上游响应读取空闲时间 |
| `timeout.max_duration_ms` | 1800000（30 分钟） | 请求最大总时长 |
| `session_policy.idle_ttl_seconds` | 86400（24 小时） | 普通粘性绑定空闲过期时间 |

耗时模型请求可参考本文示例，将响应头超时设为 180 秒；即使使用 SSE，首个响应头仍受该超时约束。超时字段单位为毫秒，范围为 1–86400000；会话 TTL 单位为秒。

普通代理请求体最大 32 MiB；管理/A2A JSON 和单个 A2A SSE 事件最大 2 MiB；Hermes JSON 请求最大 2 MiB、普通响应最大 4 MiB、单个 SSE 帧最大 2 MiB。达到并发或资源容量上限时拒绝请求，不排无限队列。

## 8. Linux 服务安装与升级

开发机可交叉构建 Linux 二进制；ARM64 服务器将 `amd64` 改为 `arm64`：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -o bin/aiagent-gateway-linux-amd64 ./cmd/gateway
```

已验收环境为 Debian 12 / systemd 252.39 / Linux arm64；其他系统版本需单独验证。复制对应架构产物后，在服务器执行：

```sh
sudo ./aiagent-gateway-linux-amd64 install
sudo aiagent-gateway status
sudo aiagent-gateway rollout status --json
```

首次安装创建专用系统用户、随机管理 Token、数据目录和 systemd 服务，并设置开机启动。消费者在创建/更新路由时动态登记；没有默认的全路由消费者。由 root 编辑 `/etc/aiagent-gateway/config.yaml` 调整管理 Token 和公开 URL，再按维护安排重启生效。

该文件使用 YAML，权限 0600，由 systemd LoadCredential 提供给服务进程读取，不作为 shell 脚本 source。运行 curl 的终端可设置自己的客户端变量。旧实验部署需在维护窗口人工停止、备份后切换，详见 [部署全流程](DEPLOYMENT_LIFECYCLE.md)。

```sh
sudo aiagent-gateway stop
sudo aiagent-gateway start
sudo aiagent-gateway restart
sudo journalctl -u aiagent-gateway.service -f
```

`stop/start/restart` 操作整套服务，可能中断连接。日常升级使用**新上传二进制**的 `install`，由后台发布控制器启动候选完整网关，验证真实流量并自动切流或回退。默认灰度分配 5% 新会话，验证至少 60 秒、每类必需请求至少 10 个成功样本，全量后再观察 300 秒；无业务流量可能因样本不足回退。

稳定时一个服务 PID，发布时最多两个，正常完成后回到一个。没有 `--update-host`。`rollout resume` 处理可恢复的 `RECOVERY_REQUIRED`；`rollout rollback` 仅允许提交前执行，COMMITTED 后必须恢复新版或另发兼容产物。完整打包上传、端口、升级、回退及降级流程见 [灰度升级操作说明](ROLLOUT_OPERATIONS.md)。

### 数据与重启行为

| 项目 | 前台单进程模式 | systemd 灰度部署模式 |
| --- | --- | --- |
| 路由和实例配置 | 保存到 `routes.json`，版本在 `config.json` | 由唯一 owner 维护同样的拆分存储 |
| 管理幂等回执和审计记录 | 保存到 `route-logs.json` | 由唯一 owner 保存到 `route-logs.json` |
| 会话、公开资源 ID、归属、数据幂等预留 | 内存保存，重启丢失 | 由共享状态日志持久化，网关换版仍定位原 Agent |
| 列表分页快照 | 重启失效 | 可随在线快照交接；冷启动后失效 |
| 已建立的 TCP/SSE 连接 | 停止进程会中断 | 旧版连接排空，新版活动流可跨状态交接；冷启动不能重建连接 |

前台模式重启后，旧 Hermes/A2A 公开 ID 不可恢复；generic 会话头再次出现则可能重新分配。上游有会话数据库也不能代替网关归属映射。需要持久归属和日常灰度升级时使用完整网关 process / systemd 部署。

单进程不能同时打开同一数据目录。完整网关灰度始终只有一个持锁 writer；数据位于 `/var/lib/aiagent-gateway`，发布报告位于其 `release/`，选版决定位于 `handoff/`。备份应包含配置、共享状态、发布记录和凭据，并在停机或其他一致性备份流程下取得。

`sudo aiagent-gateway uninstall` 停止并卸载服务/CLI，保留凭据、数据和版本产物。正式安装与 systemd 生命周期已在隔离 Linux 容器通过，实际 Agent 换版、生产压力和完整故障矩阵仍待完成，范围见 [验证记录](../VALIDATION.md)。

## 9. 常见问题

| 现象 | 检查和处理 |
| --- | --- |
| 启动提示缺配置或配置损坏 | 确认 YAML 启动配置存在且有效；路由存储缺失时会自动初始化，损坏时需恢复原文件 |
| `401 UNAUTHORIZED` | 核对端口、Token 类型和 `Authorization: Bearer ...` 格式 |
| 消费者请求返回 401 | 检查目标路由 consumers 是否已配置、Token 是否被轮换或撤销；YAML 中不再配置 callers |
| `404 ROUTE_NOT_FOUND` | 检查 `/agents/` 后是否使用 key；确认路由存在且 enabled |
| `400 SESSION_ID_REQUIRED` | generic 默认需要 `X-Session-ID`；无状态路由可关闭粘性 |
| 旧 Hermes/A2A ID 返回不存在 | 核对调用方、路由及公开 ID；前台重启丢失的映射不能直接恢复 |
| `503 NO_HEALTHY_ENDPOINT` | 确认存在 active 实例并检查目标接口健康；Hermes 的能力探测不影响调度 |
| `503 SESSION_ENDPOINT_UNAVAILABLE` | 原绑定实例禁用或目标接口不健康，恢复原实例；不会自动迁移会话 |
| `502/504 UPSTREAM_UNAVAILABLE` | 查看 `upstream.message` 中的连接/超时原因；容器中的 `127.0.0.1` 指容器自身 |
| `HERMES_UPSTREAM_ERROR` | 查看 `upstream.status` 和 `upstream.body` 中的上游状态码、错误消息和详情 |
| `428 IF_MATCH_REQUIRED` | PATCH、实例 DELETE、按内部 ID 删除路由时先读取目标资源 ETag；POST 更新和按 key 删除路由可省略 If-Match |
| `412 VERSION_CONFLICT` | 重新读取目标资源，确认变更后使用新 ETag |
| `409 ENDPOINT_IN_USE` / `ROUTE_IN_USE` | 仍有绑定、活跃请求或资源引用；先排空并检查归属 |
| `409 IDEMPOTENCY_CONFLICT` | 同键使用了不同请求；重试保持原请求，新操作换键 |
| `409 ROLLOUT_IN_PROGRESS` | 发布期间配置写入冻结，等待完成或按恢复流程处理 |
| `501` | Hermes 路径或协议能力未支持，查看对应接口说明 |
| A2A HTTP 200 但调用失败 | 检查 JSON 的 error，尤其是版本头和方法名 |
| SSE 长时间无输出或集中输出 | 检查上游是否流式、客户端是否关闭缓冲、入口代理是否缓冲以及各层超时 |

排查顺序建议为：网关 `/readyz` → 管理 `/admin/v1/config/status` → 路由及 Endpoint 详情 → 实际业务请求 → 进程/systemd 日志。灰度部署额外查看 `rollout status --json` 和完整网关服务日志。

普通 generic HTTP 代理透传上游 HTTP 状态和正文。Hermes 的 HTTP 错误响应保留网关 `error` 码，同时增加 `upstream`，例如：

```json
{
  "error": "HERMES_UPSTREAM_ERROR",
  "upstream": {
    "status": 422,
    "body": {"error": {"message": "model not found", "code": "model_not_found"}}
  }
}
```

`upstream.body` 保留 JSON 结构；非 JSON 响应使用字符串。新增的 `upstream.body` 最多保留 64 KiB 上游正文，超限时 `upstream.truncated` 为 true；读取中断时仍返回已收到的正文及 `upstream.message`。有效 A2A JSON-RPC 错误及 SSE 事件沿用 2 MiB 上限。新增详情及 A2A 错误会隐藏已配置的上游 Token、Bearer/Basic 凭据和明确的凭据字段（如 api_key、password），保留业务错误消息和诊断信息。

这些代理响应变更在 install 发布包含修复的完整产物后生效，前台部署则重启为新二进制。
