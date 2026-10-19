# AI Agent Gateway

Go 单节点 Agent 网关：HTTP 管理路由、一个路由内嵌多个实例、会话粘性、HTTP/SSE 代理，以及 A2A 1.0 JSON-RPC 转发。YAML 解析使用 go.yaml.in/yaml/v3，其余运行逻辑使用 Go 标准库。

设计见 [DESIGN.md](DESIGN.md)，开发约定见 [DEVELOPMENT.md](DEVELOPMENT.md)，管理接口契约见 [api/openapi.yaml](api/openapi.yaml)。

从启动到接入、调用和日常运维，见 [网关使用说明](docs/USAGE.md)，包含 HTTP/SSE、Hermes、A2A 示例、配置速查及常见问题。

自动灰度升级：[操作说明](docs/ROLLOUT_OPERATIONS.md)、[本次实现过程](docs/ROLLOUT_IMPLEMENTATION_RECORD.md)，包含组件改造、会话到原 Agent 的映射持久化、自动切流/回退和验证结果。架构与规划见 [通用 HTTP 网关发布设计](docs/HTTP_GATEWAY_ROLLOUT_DESIGN.md)及 [本项目实施计划](docs/AGENT_GATEWAY_ROLLOUT_IMPLEMENTATION_PLAN.md)。

首次安装生成哪些文件、运行几个进程、灰度时如何替换产物和交接请求，以及如何更新全部组件，见 [安装、运行文件与升级全流程](docs/DEPLOYMENT_LIFECYCLE.md)。

已实现 [稳定宿主 + worker](docs/PROCESS_REDUCTION_RECOMMENDATION.md)：新安装平时 2 个、灰度时 3 个进程，入口、状态和发布控制合并为一个非 root host。安装统一采用 host-worker；[单常驻进程、双进程灰度方案](docs/SINGLE_PROCESS_ROLLOUT_PLAN.md)仍是后续草案。

发版与升级步骤见 [构建打包和上传](docs/ROLLOUT_OPERATIONS.md#构建发版)、[首次安装或宿主更新](docs/ROLLOUT_OPERATIONS.md#首次安装或宿主更新)、[日常灰度升级](docs/ROLLOUT_OPERATIONS.md#日常灰度升级) 和 [异常恢复及降级](docs/ROLLOUT_OPERATIONS.md#结果确认和异常处理)。

Hermes 接入契约见 [Hermes HTTP 资源适配](docs/HERMES_HTTP_API.md)，原始来源和版本对照见 [会话接口调研](docs/HERMES_SESSION_API_RESEARCH.md)。

实施进度见 [Hermes 适配与 A2A 1.0 开发方案](docs/HERMES_A2A_V1_DEVELOPMENT_PLAN.md)。现已接入 Hermes Sessions、Runs、Chat Completions、Responses、能力发现和后台核查；A2A 仅支持 1.0，依据官方规范 v1.0.1。官方 SDK 1.2.0 的 Card 与六个 RPC 已通过双实例真实联调，最新验证范围见 [VALIDATION.md](VALIDATION.md)。

## 快速运行

需要 Go 1.24+，支持 Linux/macOS。先启动自己的 Agent 服务，再执行：

```sh
cp config.example.yaml config.yaml
chmod 600 config.yaml
# 编辑 config.yaml，替换示例 Token 和地址。
go run ./cmd/gateway --config ./config.yaml
```

首次与后续启动使用同一条命令：路由存储不存在时自动初始化，已有文件直接读取，损坏时拒绝启动。省略 `--config` 默认读取当前目录的 `config.yaml`，相对数据路径以 YAML 所在目录为准。旧 `GATEWAY_*` 启动变量及上游凭据环境变量均不再读取；完整字段、上游凭据和旧部署迁移见 [YAML 配置说明](docs/CONFIGURATION.md)。部署时持久化数据目录，TLS 由入口代理终止。

以下仅为 curl 客户端设置 shell 变量，值须与 YAML 中的 Token 一致；网关不会读取这些变量：

```sh
GATEWAY_ADMIN_TOKEN='replace-with-admin-secret'
GATEWAY_CALLER_TOKEN='replace-with-caller-secret'
```

创建一个普通代理路由：

```sh
curl http://localhost:9090/admin/v1/routes \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: research-v1' \
  -d '{
    "key":"research",
    "consumers":[{"id":"app-a","token":"replace-with-caller-secret"}],
    "agent_type":"generic",
    "expose":["proxy"],
    "endpoints":[
      {"name":"instance-1","interfaces":{"proxy":{"url":"http://127.0.0.1:8001"}},"weight":100},
      {"name":"instance-2","interfaces":{"proxy":{"url":"http://127.0.0.1:8002"}},"weight":100}
    ]
  }'
```

路由默认启用会话粘性，同一调用方使用相同会话 ID 会命中原实例：

```sh
curl -N http://localhost:8080/agents/research/chat \
  -H 'Authorization: Bearer replace-with-caller-secret' \
  -H 'X-Session-ID: session-001' \
  -H 'Content-Type: application/json' \
  -d '{"message":"hello"}'
```

若上游无需会话粘性，创建路由时传 `"session_policy":{"enabled":false}`。上游 URL 的路径作为前缀，查询参数合并保留。上游 Token 直接配置在 Endpoint 的 `interfaces.proxy.token` 中，随路由管理更新即时生效，管理响应不回显；客户端 Authorization、Cookie 和身份转发头会被清理。

普通 HTTP 代理透传上游错误状态及正文；Hermes 错误通过 `upstream.status/body` 返回详情，连接失败通过 `upstream.message` 返回具体原因。A2A 保留上游 `error.message/data`，非 JSON-RPC 的 HTTP 错误附在 `error.data.upstream`。新增错误详情对凭据脱敏，HTTP 错误正文最多 64 KiB；响应示例见 [常见问题](docs/USAGE.md#9-常见问题)。

## 同一 Agent 的聊天与 A2A

以下配置可同时接入普通 HTTP 服务和 A2A 1.0 服务，两个地址必须属于同一个 Agent 部署：

```json
{
  "key": "research",
  "consumers": [{"id":"app-a","token":"replace-with-caller-secret"}],
  "agent_type": "generic",
  "expose": ["proxy", "a2a"],
  "endpoints": [{
    "name": "instance-1",
    "interfaces": {
      "proxy": {"url": "http://127.0.0.1:8001"},
      "a2a": {"url": "http://127.0.0.1:9900/", "protocol_version": "1.0"}
    }
  }]
}
```

每个实例都必须提供所有开放的接口，同一路由的 A2A 版本必须一致。聊天访问 `/agents/research/{原生路径}`，兼容聊天客户端的 base URL 可设为 `/agents/research/v1`。`/a2a`、`/.well-known/*` 是保留路径，错误方法、未知子路径和关闭的入口不会进入普通代理。

A2A 升级是协议变更：schema 1 或旧管理形态中未声明版本的 A2A 接口迁移为 `1.0`，对应上游也必须升级；schema 2 明确声明 `0.3.0` 的配置会拒绝加载/保存，需升级上游后将其改为 `1.0`。不会自动转换旧报文。schema 1 首次写入前仍保留原始备份，已有幂等回执保持原样。

兼容期保留 `/proxy/{key}/{path...}` 和 `/a2a/{key}`，直接复用相同处理器，不重定向 POST 或 SSE。同名粘性 ID 在 proxy 与 A2A 中分别绑定，不承诺跨协议续接同一会话。

`agent_type: "hermes"` 使用显式路径表和调用方资源归属，提供原生会话、Runs、兼容聊天及 SSE。创建时的客户端 ID 是别名，后续使用响应中的公开 ID；未支持的 jobs、文件/浏览器和系统管理接口返回 501。`generic` 保留原有透传。完整请求约束见 [Hermes HTTP 资源适配](docs/HERMES_HTTP_API.md)。

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

目标服务器需要运行 systemd 247+（支持 LoadCredential），并提供 `systemctl`、`getent`、`groupadd` 和 `useradd`。安装需要 root；运行服务使用独立的 `aiagent-gateway` 系统用户。二进制内嵌所需 unit 文件，目标机器无需 Go、源码或额外脚本。

安装命令完成以下工作：

- 首次安装或宿主更新将管理 CLI 放到 `/usr/local/bin/aiagent-gateway`；后续灰度升级保留该 CLI，通过新上传二进制的 `install` 提交业务版本。
- 首次安装生成随机管理 Token，保存于 `/etc/aiagent-gateway/config.yaml`，权限 0600；Token 不输出到终端。
- 安装时预先创建数据目录中的 `config.json`、`routes.json` 和 `route-logs.json`；前台和状态服务启动时也会自动初始化全新的路由存储。
- 新安装生成部署清单和单个 host service，由 host 启动/回收业务 worker；正常运行 2 个进程，灰度 3 个。通过 `/readyz` 检查，启用开机自启动；host 异常退出由 systemd 清理子进程后重启。
- 已迁入灰度架构后，安装不同版本会启动另一个端口的 worker，以真实 HTTP/SSE/A2A 代理结果验证后自动切流，失败自动回退；同一 SHA-256 版本已健康运行时直接成功返回，正在发布时接续等待原任务。配置、资源归属和并发额度由共享状态服务统一管理。
- 宿主自身升级在停机后执行 `install --update-host`，先完整备份，保留业务状态，并以新产物建立发布基线。Linux systemd 实机部署尚待验收，见 [灰度操作说明](docs/ROLLOUT_OPERATIONS.md)。

默认数据面为 `:8080`，管理面为 `:9090`，均监听所有网卡。远程管理使用 `http://<网关IP或域名>:9090`，请求携带管理 Token；自定义 `admin_port` 时同步调整访问端口。

消费者只在路由 consumers 中动态登记，不生成默认全路由消费者。通过 root 编辑 YAML 调整管理 Token 和 `public_url`；后者应改为客户端实际可访问的地址。上游 Token 在路由各接口的 `token` 中动态配置。unit 通过 LoadCredential 将配置交给服务进程读取，原文件维持 root 专属权限，不作为 shell 脚本执行。启动字段见 [配置说明](docs/CONFIGURATION.md)。

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

消费者凭据只在路由正文 `consumers` 中配置，YAML 不接受 `callers`。例如对已有路由 POST `{"key":"research","consumers":[{"id":"app-a","token":"new-secret"}]}` 即可立即轮换 Token；省略 consumers 保留原列表，传 `[]` 撤销全部。保持消费者 ID 可续接原资源；GET/POST 只返回 ID，Token 只保存哈希。详细规则见 [动态消费者](docs/USAGE.md#动态配置消费者)。

- `POST /admin/v1/routes`：正文 `key` 不存在则创建（201），存在则更新（200），默认不需要 `If-Match`。
- `GET /admin/v1/routes`：分页查询路由。
- `GET/DELETE /admin/v1/routes/{key}`：按 key 获取、删除；删除默认不需要 `If-Match`，有引用时仍返回 409。
- `/admin/v1/routes/{key}/endpoints`：POST 添加、GET 分页查询实例。
- `/admin/v1/routes/{key}/endpoints/{endpointID}`：GET、PATCH、DELETE。
- `/admin/v1/routes/{key}/sessions`：普通粘性绑定查询；`/tasks`：A2A 任务查询。
- `/admin/v1/routes/{key}/resources`、`/runs`：公共资源与 Hermes Run 归属查询，支持 `owner_id`、`endpoint_id`、`interface` 筛选；`/resources` 另支持 `resource_kind`。这些接口需要管理 Token。
- `/admin/v1/config/status`：内存版本及持久化健康状态。
- `/admin/v1/audit-logs`、`/metrics`：审计、Prometheus 文本指标，均需管理 Token。

POST 更新保留未提供的路由字段；提供 `endpoints` 时按完整列表替换，同名实例保留 ID 和绑定，列表内每项按完整实例配置处理。使用稳定且唯一的实例 `name`；未命名实例仅能按完全相同的 interfaces 匹配，歧义返回 409。删除、换地址或移除接口仍受引用保护。

分页参数为 `offset`、`limit`（默认和最大均为 100）。详情 GET 返回 ETag；POST 更新和按 key 删除可选传 `If-Match`，传入但不匹配返回 412。原内部 ID 路径与路由 PATCH 保留兼容，PATCH、实例 DELETE 及按内部 ID 删除路由仍必须使用该资源的 `If-Match`。路径优先匹配 key，无匹配时才尝试内部 ID。POST 支持 `Idempotency-Key`，相同键和相同原始请求重放原状态码及结果，不同请求返回 409；新的更新操作必须换幂等键。保存最近 1000 条幂等记录与 1000 条审计。

例如直接按 key 修改超时、查询和删除无需引用的路由：

```sh
curl -fsS http://127.0.0.1:9090/admin/v1/routes \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"key":"research","timeout":{"response_header_ms":180000}}'

curl -fsS http://127.0.0.1:9090/admin/v1/routes/research \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN"

curl -fsS -X DELETE http://127.0.0.1:9090/admin/v1/routes/research \
  -H "Authorization: Bearer $GATEWAY_ADMIN_TOKEN"
```

上述部分更新示例要求 `research` 已存在；首次创建还需提供 `expose` 等必要配置。启动参数只从 YAML 读取，环境变量已弃用。管理 API 自动维护三份存储：`config.json` 只保留版本信息，`routes.json` 保存路由和内嵌实例，`route-logs.json` 保存管理幂等回执和审计记录，后两类记录各保留最近 1000 条。文件布局及兼容说明见 [路由存储文件](docs/CONFIGURATION.md#路由存储文件)。

前台模式使用新二进制启动即可启用本次管理接口。灰度部署的管理接口由 host 提供，现有部署需按基础组件维护流程更新宿主；日常 install 只替换业务 worker，不会自动升级管理接口。

配置先提交 `.config.pending.json` 恢复事务，再逐份原子替换三份文件并同步目录，清理事务后发布内存快照。文件和目录权限默认 0600 / 0700。前台单进程模式下，多个进程不能同时打开同一数据目录；灰度模式由单个状态服务独占目录，worker 通过本机 Socket 访问；恢复事务提交后任何写入失败都会使 ready 失败，需排查存储并重启完成恢复。

实例状态支持 active、draining、disabled。draining 不接新会话，已有会话继续访问。有绑定或活跃调用的实例不能删除或替换地址。每个 `interfaces` 项的 `health_url` 可指定主动健康检查地址，默认每 10 秒串行检查，携带该接口的凭据；仅 2xx 表示健康，不跟随重定向。未配置探活或首次探活前默认可调度。两个接口健康互不影响，HTTP 并发共同计入实例上限；实例详情 GET 的 `runtime` 展示接口健康、各接口/实例 HTTP 并发和引用数。

## 配置升级与回滚

新数据目录初始化为 `schema_version: 2`。读取 schema 1 时，仅在内存迁移为 `generic`、单项 `expose` 和对应 `interfaces`；ID、配置版本和旧幂等回执保持不变。启动和幂等重放不会改写旧文件。

首次成功管理写入前，将原始单文件配置保存为数据目录中的 `config.schema-1.backup.json` 或 `config.schema-2.backup.json`（0600），再事务提交 schema 2 的拆分存储。备份或事务提交前失败保留原文件及内存快照；事务提交后的写入失败会使网关退出 ready 状态，重启完成同一事务。已有同名备份内容不同或为符号链接时拒绝迁移，需先保留并移走冲突备份。

管理 API 接受纯旧形态创建，返回新形态；`protocol` 与新字段混用返回 409。旧 Endpoint 的 `url/token/health_url` 创建或 PATCH，以及旧 `protocol` PATCH，只适用于 `generic` 且 `expose` 为单项的路由；双入口使用新字段更新。旧幂等回执继续重放原响应，新回执使用新形态。

回滚存储格式需先停止网关、备份整个数据目录，再将拆分文件和未完成事务移入备份目录，同时恢复旧二进制和对应 schema 备份为 `config.json`。旧二进制无法读取拆分存储；备份不包含升级后的配置修改。迁移不修改凭据，前台模式的运行时绑定仍不跨重启恢复；灰度模式使用独立共享状态日志，不应通过恢复旧配置文件回滚业务版本。

## A2A

创建路由时将 `expose` 设为 `["a2a"]`（或与 `"proxy"` 同时启用），在 `interfaces.a2a` 中指定准确的 JSON-RPC `url` 和 `protocol_version: "1.0"`。可传入描述性 `agent_card`；省略时默认名称/描述取路由 key、skills 为空、不声明 streaming。提供自定义 Card 时需完整提供以下字段：

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

Card 位于 `/agents/{appId}/.well-known/agent-card.json`，JSON-RPC/SSE 位于 `POST /agents/{appId}/a2a`，其中 `appId` 是路由 `key`。两者均需要调用方认证；Card 只宣告新的 RPC 地址，网关改写认证声明和协议版本。设置 YAML `public_url` 为客户端实际可访问的网关基础地址。

支持 `SendMessage`、`SendStreamingMessage`、`GetTask`、`ListTasks`、`CancelTask`、`SubscribeToTask`。请求必须携带 `A2A-Version: 1.0`；缺失、0.3/0.3.0 或其他版本返回 `-32009`，旧方法名不再支持。任务和 context 使用网关生成的 ID，隔离不同调用方及实例的同名 ID；协议数据中的文件、parts 和 metadata 保持原样。已有任务与 context 必须携带网关返回的标识；未知任务返回协议错误，不随机转发。

请求示例（先创建路由并配置 A2A 1.0 上游）：

```sh
curl -X POST http://localhost:8080/agents/research/a2a \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
  -H 'Content-Type: application/json' -H 'A2A-Version: 1.0' \
  -d '{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m-1","role":"ROLE_USER","parts":[{"text":"你好"}]},"configuration":{"returnImmediately":true}}}'
```

`SendMessage` 返回 `result.task` 或 `result.message`；默认等待终态或输入/认证等待，`returnImmediately: true` 允许返回进行中的任务。角色和状态使用 `ROLE_*`、`TASK_STATE_*`，Part 使用 `text/raw/url/data` 单选字段。流以 Task 或单个 Message 开始，随后仅接受同一任务的 statusUpdate/artifactUpdate；终态关闭流，中途异常发出协议错误并保留已有归属，等待输入/认证不视为终态。

Card 使用 `supportedInterfaces`、`httpAuthSecurityScheme` 和 `securityRequirements`，仅宣告网关 JSONRPC 地址及已实现能力。公开接口不设置 tenant；若上游需要，配置 `interfaces.a2a.tenant`，网关注入固定值，不允许调用方指定其他 tenant。有资源引用的端点不能修改 tenant。

`ListTasks` 只查询当前调用方和路由已登记任务，逐个到原实例执行 GetTask，不转发上游全局列表。支持 contextId/status/statusTimestampAfter 过滤及 historyLength/includeArtifacts；按状态时间倒序，默认不含 artifacts、历史消息上限为 0（可显式指定）。pageSize 默认 50、范围 1–100；nextPageToken 始终存在，末页为空字符串。分页快照绑定调用方、路由版本和筛选条件，有效期 2 分钟，重放同页不重复访问上游。最多并发 4 个列表请求、每快照 1000 个候选任务/8 MiB、每页 2 MiB、全局 64 个快照/32 MiB；超限可缩小 contextId/pageSize/historyLength。旧游标过期或条件改变返回参数错误。

JSON-RPC 协议错误通过 HTTP 200 的 error 对象返回，认证/路由/HTTP 方法错误仍使用 HTTP 状态。上游错误保留数值 code，message 使用通用协议说明，不转发可能包含内部资源标识或凭据的 error.data。

不支持 REST/gRPC 绑定、推送通知、扩展 Card、任务编排或普通 HTTP Agent 到 A2A 的通用转换。调用方可在首次请求通过 `X-Session-ID` 提供稳定会话键。A2A 任务和 context 映射只存内存：重启后旧标识不可恢复，非终态任务保留映射，终态任务与无任务引用的 context 空闲 24 小时后清理。

## 资源目录与生命周期

A2A 1.0 task/context 现已使用公共目录，按调用方、路由、资源类型隔离；同名上游 ID 在不同实例中保持独立。每个 JSON/SSE 事件的映射先完整检查引用和容量，再一次性登记并改写输出，失败不会留下部分映射。查询持有短期资源引用，HTTP 传输期间不持有目录锁。

目录已为 Hermes session/run/response 提供随机上游会话 ID、客户端别名、压缩谱系、父子关系和数据幂等预留接口。并发首次创建固定同一实例；同键不同指纹拒绝；请求指纹包含正文及适配器指定的相关请求头全部值。上游幂等键按调用方、路由、接口和操作隔离。Hermes HTTP 已接入这些接口；Runs 的 Idempotency-Key 固定原实例并交给上游去重，不承诺跨网关重启的 exactly-once。

状态为 `reserved`、`live`、`waiting`、`unknown`、`terminal`。未发送的预留可撤销；适配器须在发送前标记 `unknown`，结果不明时保留原实例。非终态 Run/Task、未知状态、审批等待和有活跃请求/子资源引用的资源不按普通空闲 TTL 清理；终态和可清理资源默认空闲 24 小时后回收，别名与反向索引一起移除。迟到事件不使终态任务重新变为运行中。

父资源引用覆盖后台执行，返回 202 或结束 SSE 不意味着实例可删除。排空实例可继续已知资源请求和已有会话的新 Run/response；fork 新会话被拒绝。目录和预留仅在内存，重启后已有公开资源 ID 返回不可恢复错误，不能随机重选实例。A2A ListTasks 和 Hermes 会话列表均提供调用方隔离的有界快照分页。后台核查独立于健康检查，每 10 秒轮转扫描最多 16 个资源，每次查询上限 3 秒；暂时失败保留原归属及未知状态。

## 性能与运行边界

路由快照原子读取、会话索引使用 256 个分片、已有会话按实例 ID 查找；新会话按权重轮询。HTTP 连接和复制缓冲复用，默认没有应用层重试。普通代理请求体上限 32 MiB；管理/A2A JSON 和单个 A2A SSE 事件上限 2 MiB。Hermes JSON 请求上限 2 MiB、响应 4 MiB、单帧 2 MiB。SSE 使用独立的空闲/最大时长控制，普通流的客户端断开会取消上游 HTTP 请求；已确认的 Hermes A2A returnImmediately 请求由网关继续读取有界后台流，显式取消使用 CancelTask。

默认全局并发 4096、每调用方 1024、每实例 256，会话和任务容量各 100000。超限返回不可用，不排无限队列。上限可通过 `max_inflight`、`max_per_caller`、`max_per_endpoint`、`max_sessions`、`max_tasks` 调整。会话空闲 TTL 默认 24 小时，活跃请求不会被过期清理。公共目录另有 `max_resources`（默认 200000）、`max_resource_aliases`（200000，客户端别名及额外上游谱系 ID）、`max_data_idempotency`（100000）容量上限。目录内 session/context 共享 `max_sessions` 上限，Run/Task 共享 `max_tasks` 上限；generic 粘性表仍单独计数。`/metrics` 增加资源、别名和数据幂等预留数量。

首版提供 Prometheus 基础计数和进程启动/停机结构化日志；OpenTelemetry、延迟直方图、被动熔断和完整访问日志尚未接入。主动健康检查串行执行，大规模实例部署前需要压测并改为有界并发。公共资源目录目前采用独立互斥锁，批量映射与并发测试已覆盖；大规模资源/列表查询和 A2A 高并发场景需进一步压测。设计文档中的全部生产运行保障不等于已经实现。

本机环境现使用 `hermes-local` 的 Hermes 专用适配器和两个独立实例：HTTP 18642/18643、A2A 19900/19901，公开网关仍为 18080。启动、原生资源检查及官方 SDK 检查见 [部署说明](deploy/hermes-local/README.md)。历史 generic 联调结果和本轮执行结果分别保存在验证记录中。

## 验证

```sh
go test ./...
go test -race ./...
go vet ./...
go test ./internal/gateway/runtime -run '^$' -bench . -benchmem
go build -o bin/gateway ./cmd/gateway
```

目录职责和测试位置见 [Gateway 代码结构](internal/gateway/README.md)和 [集成测试说明](tests/integration/README.md)。`GATEWAY_TEST_IN_MEMORY=true go test -race ./internal/gateway/...` 可让包内 HTTP 上游使用 net.Pipe，共享状态测试仍需要本地 Unix Socket；`tests/integration` 使用本地 TCP 和 Unix Socket，不受该变量影响。各阶段的真实 TCP 竞态回归、官方 SDK 双实例联调和当前检查结果见 [VALIDATION.md](VALIDATION.md)。目标机器容量压测、Linux systemd 实机验收及 P6 跨重启资源恢复仍是独立工作项。
