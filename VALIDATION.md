# 验证记录


## 自动灰度升级实现（2026-10-06）

新增固定入口、后台发布控制器、共享资源状态、真实代理结果协议和 systemd 双 worker 安装流程。操作方式与边界见 [灰度升级说明](docs/ROLLOUT_OPERATIONS.md)。本次没有修改任何真实 Linux 服务或既有 Hermes 部署。

| 验证 | 结果 |
| --- | --- |
| `go test ./...`、`go test -race ./...` | 通过，覆盖既有网关及新增发布/状态/安装测试 |
| `go vet ./...` | 通过 |
| Linux amd64 / arm64 静态交叉构建 | 通过，产物位于临时目录；未据此宣称 Linux 实机部署通过 |
| 四个真实进程：state、entry、worker A/B | 临时目录和真实 loopback TCP 验证通过；100 个新会话中 5 个进入候选，全量后旧会话保持 Agent 归属，失败 POST 不重放，回退后可续接 |
| 文档链接、JSON 示例、Python AST、`git diff --check` | 通过 |

自动测试新增覆盖：成功转正及旧版排空，代理失败/样本不足/必需 profile 缺失/结果丢失导致回退，全量观察期故障，排空超时及 resume，切流已应用但回执丢失后的 revision 恢复；目录独占、日志尾记录截断、完整记录损坏拒绝及压缩恢复；双 worker 并发首绑定、共享限额、资源幂等/unknown 保护、持久资源恢复、Hermes 会话续接和流式请求、A2A 资源映射和列表快照共享、调用方隔离；HTTP 200 协议错误、SSE 提前结束、长流切换不中断、内部结果头不暴露；升级不停止旧服务、不改凭据和基础 CLI、旧架构显式迁移、不可变产物校验及卸载保留恢复数据。

多进程检查由 `deploy/rollout-local/check.py` 执行，使用本次构建的二进制和模拟 HTTP 上游。该脚本验证实际进程/网络切流，自动发布状态机由 Go 控制器测试验证；不是 Linux systemd 安装验收，也不是两版真实模型兼容性验收。

仍待完成：Linux/systemd 全新安装与维护迁移、实际 unit 加载/权限/开机恢复、真实 Agent 在升级/回退期间的全链路联调、生产压力和长时间运行验证。基础组件无损升级、无限 SSE 的自动成功判据、主动业务探测及另一个 HTTP 网关的实际接入不在本次完成范围。短期列表游标仅跨 worker 共享，不跨状态服务重启。

## Hermes 专用适配与双实例验收（2026-10-05）

当前路由 `hermes-local` 使用 `agent_type: hermes`，同一路由配置两个独立 Hermes 实例，HTTP/A2A 使用各自凭据。固定镜像 `nousresearch/hermes-agent:v2026.9.24`，摘要 `sha256:fca358f12efd65bfaaca05884166f15c0e2788375ca30d77061ac1ebc96452b7`；模型服务为 `http://www.tf1997.store:9091/v1`。以下由代理直接执行；后续各节为历史记录，当时“待实现/待验证”不代表当前状态。

已实现 Sessions、Runs、Chat Completions、Responses 的公开 ID 和调用方归属、幂等与原实例续接、有界会话列表、按接口能力发现、独立后台状态核查。Hermes A2A 适配补齐订阅首 Task，并处理上游忽略 returnImmediately 的行为。完整契约见 [HTTP API](docs/HERMES_HTTP_API.md)。

| 验证 | 结果 |
| --- | --- |
| `GOCACHE=/private/tmp/aiagent-gateway-go-cache go test -race ./...` | 全部通过；gateway 3.446s，cmd/service 命中缓存 |
| `go vet ./...`、Python 语法、OpenAPI JSON/180 个本地引用、文档链接、`git diff --check` | 通过；运行凭据和报告已确认由 Git 忽略 |
| 本机启动构建、Linux amd64/arm64 交叉构建 | 通过；交叉构建不代表 Linux 实机部署验证 |
| 综合 HTTP/SSE/A2A 检查 | preflight、http_sessions、http_chat_sse、a2a_context、released 全部通过；两轮回忆和最终引用释放通过 |
| 官方 a2a-sdk 1.2.0，Python 3.13 | Card、六个 RPC、两实例分配、异步返回、订阅首 Task、取消及取消事件通过；pip check 通过 |
| 原生双实例资源 | 调用方隔离、公开 ID/原始 ID 边界、冲突、列表游标、Runs 幂等与 steer/stop、Responses 同步和 SSE 续接通过 |
| 排空与故障 | 已有会话留在原实例、新建转向可用实例、fork 与删除引用保护、故障会话不迁移、容器恢复后原会话可访问通过 |
| 网关重启 | 配置和两个端点保留；旧 session 返回 404，旧 A2A task 返回 -32001，通过 |

最终综合报告 `integration-all.json` 为 `passed: true`，执行时间 15:04:40–15:06:43 +08:00。

私有报告位于 `deploy/hermes-local/.runtime/gateway/`，已由 Git 忽略。`integration-sdk.json` 执行时间为 14:35:47–14:36:31 +08:00；`integration-native.json` 为 15:01:00–15:02:06 +08:00；`integration-restart.json` 为 14:39:53 +08:00。后续网关重启使报告内的运行时资源 ID 失效，不影响记录的测试结果。

发布版差异与验收边界：

- Runs events 使用一次性消费队列，完成订阅后再次 GET 返回 404。实际报告 `replay.supported: false`，能力发现 `run_events_replay: false`。先前按调研快照假设序号回放的检查失败，核对固定容器源码后改为验证真实版本边界；未宣称回放通过。
- 首轮专用适配综合检查中，A2A 第二轮模型回答“无法提供”，资源映射与实例归属一致。该失败报告另存 `integration-hermes-first.json`；后续使用明确标注为公开虚构标签的 recall 提示。
- 审批、压缩谱系、容量和异常流等有 Go 回归覆盖，不宣称都在真实模型中触发。生产压力、Linux systemd 实机部署和跨协议共享同一会话不在本次真实验收范围。
- P6 资源归属持久化未纳入本期。网关重启后拒绝旧公开资源 ID；不能凭未知命名 conversation 恢复旧链，严格续接应使用已知 previous_response_id 或显式 session。


## 本机网关接入与真实 Hermes 联调（2026-10-04）

新增 `deploy/hermes-local/gateway.py` 和 `check_gateway.py`：独立网关启动/停止/重启、上游凭据加载、带版本条件的路由注册，以及真实 HTTP/SSE、会话和 A2A 联调检查。Go 核心代码未在此步骤修改。

| 检查 | 结果 |
| --- | --- |
| 两个 Python 脚本 AST 语法 | 通过 |
| 内存 SSE 输入：LF/CRLF、多行 data、注释、DONE、未闭合帧和大小限制 | 通过；未访问网络 |
| 模拟 HTTP 非 JSON 错误、路由创建/重复执行/ETag 更新、端点冲突和禁用路由拒绝、结果文件 0600 | 通过；使用临时目录和模拟响应，未注册真实路由 |
| `GOCACHE=/private/tmp/aiagent-gateway-go-cache GATEWAY_TEST_IN_MEMORY=true go test -race ./...` | 通过，三个包均命中缓存 |
| `GOCACHE=/private/tmp/aiagent-gateway-go-cache go vet ./...` | 通过 |
| 真实 Hermes HTTP `/health` | 代理只读访问通过：`status: ok`、`platform: hermes-agent`、版本 `0.21.5` |
| 独立本机网关启动和路由注册 | 用户手动执行后，代理读取本地启动日志与 `registered-route.json` 确认成功：监听 18080/19090，路由启用，A2A streaming 为 true |
| 经网关的模型/SSE/会话/A2A 检查 | 全部五项通过；用户在本机执行并提供终端结果，代理核对本地 `integration-all.json` 的完整报告 |

网关启动日志时间为 2026-10-04 20:51:25 +08:00，路由 `hermes-local` 已注册，响应头/流空闲/总时长超时分别为 180/120/300 秒。启动与联调命令最初被自动审批服务 HTTP 404（审核模型不可用）阻挡，用户随后在本机执行：

```sh
python3 deploy/hermes-local/check_gateway.py
```

终端输出依次为 `PASS preflight`、`PASS http_sessions`、`PASS http_chat_sse`、`PASS a2a_context`、`PASS released`，最终 `通过：all`。代理已读取 `deploy/hermes-local/.runtime/gateway/integration-all.json`，确认 `section: all`、`passed: true`，运行 ID `f8370cf574f7`；运行时间为 2026-10-04 21:35:53 至 21:38:24 +08:00。报告与凭据保留在 Git 忽略目录中。

| 真实检查 | 报告结果 |
| --- | --- |
| preflight | capabilities 可访问，Card 公开地址为 `http://127.0.0.1:18080/agents/hermes-local/a2a`，A2A 1.0 与 streaming 检查通过 |
| HTTP 原生 session | 创建、同步首轮、第二轮 SSE 回忆随机代码均通过；历史含 4 条消息；收到 assistant.completed、run.completed 和 done |
| Chat Completions SSE | 收到预期文本、finish reason 和 `[DONE]`；共 12 个事件 |
| A2A context | 同步任务与同 context 的新流式任务均完成，第二轮 recall 成功；GetTask、两页 ListTasks 和另一调用方隔离通过 |
| 释放 | `http_inflight: 0`；非终态任务/Run、预留资源和未知资源计数均为 0 |

原生 session SSE 首事件 0.017 秒、总耗时 52.138 秒；Chat Completions SSE 首事件 0.003 秒、总耗时 4.721 秒；A2A SSE 首事件 0.003 秒、总耗时 6.726 秒，状态为 submitted → working → completed。这是单次联调观察，首事件可为生命周期或角色事件，不等于首个模型文本耗时或容量指标。

复现步骤见 [部署说明](deploy/hermes-local/README.md)。当前 HTTP 使用 generic 透传，原生 Hermes session 归属隔离、Runs、真实任务取消/订阅恢复、多个 profile/实例和官方 SDK 仍不在本次验收内。

## Hermes Docker 部署与冒烟验证（2026-10-04）

新增 [部署说明和启动脚本](deploy/hermes-local/README.md)，配置模型服务 `http://www.tf1997.store:9091/v1`，使用官方 `nousresearch/hermes-agent:v2026.9.24` 及固定镜像摘要。默认只在本机开放 HTTP 18642、A2A 19900，独立持久化 default profile，运行凭据与数据均被 Git 忽略。

已验证：官方 Docker Hub 标签存在且包含 arm64/amd64；Docker Compose 2.37.1 配置语法与渲染通过；Python 语法、模拟模型响应下的配置生成/重复执行保留凭据、0600 文件权限、特殊字符原始 env 文件、文档链接与 `git diff --check` 通过。模拟检查未启动容器，也未真实调用模型。

2026-09-30 准备阶段，代理访问本机 Docker socket 被沙箱限制，自动审批服务返回 HTTP 404（审核模型不可用）；无凭据探测模型 `/v1/models` 返回 HTTP 401 `API_KEY_REQUIRED`。随后用户手动完成启动，并于 2026-10-04 提供以下结果：

```text
Network aiagent-hermes-local_default     Created
Container aiagent-hermes-local-hermes-1  Healthy  6.0s
通过：HTTP health、认证 capabilities/sessions、Hermes 模型调用、A2A 1.0 Card/GetTask。
HTTP endpoint: http://127.0.0.1:18642
A2A endpoint:  http://127.0.0.1:19900/
```

该结果来自用户提供的终端输出，确认真实 Hermes 容器部署及直接访问上游的基础冒烟检查通过。网关专用凭据位于 `deploy/hermes-local/.runtime/gateway.env`，仍由 Git 忽略。A2A GetTask 检查使用不存在的任务，验证认证和 TaskNotFound 响应；尚未据此确认经 aiagent-gateway 的消息/SSE、会话续接、Runs、多个 profile 或多个实例的完整互操作。

## A2A 1.0 替换验证（2026-09-30，历史代码）

按用户决定移除 0.3.0 编解码器，只支持协议 `1.0`。官方最新稳定发布核对为 `v1.0.1`（2026-05-28），schema 固定于 commit `3303592588e388e62e0f69f701af531d2f4e3991`。原始 proto、许可证、SHA-256 和派生 JSON fixtures 位于 [testdata/a2a-v1.0.1](internal/gateway/testdata/a2a-v1.0.1/README.md)。本节记录当时变更；当前验收见文首 2026-10-05 记录。

| 检查 | 结果 |
| --- | --- |
| `GOCACHE=/private/tmp/aiagent-gateway-go-cache GATEWAY_TEST_IN_MEMORY=true go test ./...` | 通过 |
| 同环境 `go test -race ./...` | 最终代码通过；gateway 包 5.056s，其余两个包命中缓存 |
| `GOCACHE=/private/tmp/aiagent-gateway-go-cache go vet ./...` | 通过 |
| 本机 `go build -o bin/gateway ./cmd/gateway` | 通过，使用上述 GOCACHE |
| `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/aiagent-gateway-linux-amd64 ./cmd/gateway` | 通过，使用上述 GOCACHE；未在 Linux 运行 |
| OpenAPI JSON/180 个本地引用、文档 JSON 与相对链接、`git diff --check` | 通过 |
| 本次代码的真实 TCP `go test -race ./...` | 用户在本机手动执行并提供结果：通过；gateway 包 2.580s，其余两个包命中缓存 |
| 官方 SDK、经网关的真实 Hermes 完整互操作 | 2026-10-04 已补齐上文单实例 HTTP/SSE、会话及 A2A 基础联调；官方 SDK 与其余完整验收仍待执行 |

新增和迁移的测试覆盖：1.0 六个 RPC 方法、Card 接口/认证声明、缺失/旧版本拒绝、旧方法和旧 Part/角色/状态拒绝、默认阻塞与 returnImmediately、tenant 固定注入和引用保护、数字精度与业务数据不改写、task/message 单选包装与请求 ID 匹配、标准错误码与错误细节脱敏；任务原实例的查询/取消/订阅、终态拒绝和调用方隔离；SSE 首帧、多行、CR/LF/CRLF、任务身份变化、artifact/status 更新、等待与终态、未闭合事件和提前 EOF、客户端取消传播；任务列表排序/筛选/历史与 artifacts 控制、调用方/条件绑定游标、同页重放、过期、候选任务和并发容量、上游失败不重新调度。保留已有管理、generic 代理、迁移和公共资源目录回归。

代理的真实 TCP 测试申请曾因自动审批服务 HTTP 404（审核模型 `gpt-5.6-luna` 不可用）未能执行，未绕过审批。随后用户在本机项目目录手动执行以下命令，并提供本次 A2A 1.0 代码的通过结果：

```sh
GOCACHE=/private/tmp/aiagent-gateway-go-cache go test -race ./...
```

```text
ok      aiagent-gateway/cmd/gateway     (cached)
ok      aiagent-gateway/internal/gateway        2.580s
ok      aiagent-gateway/internal/service        (cached)
```

该结果来自用户提供的终端输出，补齐本次 A2A 1.0 的本机真实 TCP 竞态验证，与下方 P2 的历史结果分别记录。

Hermes HTTP 适配、能力发现、主动后台核查、官方 SDK 及其余 Hermes 验收仍待后续阶段；A2A 资源和列表快照不跨重启恢复。


## P2 公共资源目录验证（2026-09-30）

本阶段新增公共资源目录并替换 A2A 0.3.0 独立 task/context Map，提供 Hermes session/run/response 的归属、别名、父子关系和幂等预留基础。Hermes HTTP 处理器、A2A 1.0、调用方列表游标和主动后台核查尚未接入。

| 检查 | 结果 |
| --- | --- |
| `GOCACHE=/private/tmp/aiagent-gateway-go-cache GATEWAY_TEST_IN_MEMORY=true go test ./...` | 通过 |
| 同环境 `go test -race ./...` | 最终代码通过，三个包均成功 |
| `GOCACHE=/private/tmp/aiagent-gateway-go-cache go test -race ./...`（真实 TCP） | 用户在本机手动执行并提供结果：通过；gateway 包 3.315s，另外两个包命中测试缓存 |
| `GOCACHE=/private/tmp/aiagent-gateway-go-cache go vet ./...` | 通过 |
| 本机 `go build -o bin/gateway ./cmd/gateway` | 通过，使用上述临时 GOCACHE |
| `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/aiagent-gateway-linux-amd64 ./cmd/gateway` | 通过，使用上述临时 GOCACHE，未在 Linux 执行 |
| OpenAPI JSON、180 个本地引用、文档 JSON 示例/链接、`git diff --check` | 通过 |

新增测试覆盖：64 个并发首次创建只生成一份归属，跨 caller 的公开/上游 ID 和幂等键隔离，同键不同指纹拒绝；客户端与谱系别名冲突、父子引用、引用计数释放、generation 拒绝旧响应；容量不足不留下资源/别名/幂等残留，明确未发送时可撤销，结果不明时不能撤销；排空实例拒绝 fork、允许已有会话新 Run，已有资源故障不重选，资源引用保护管理删除/换址；A2A 批量映射失败不改原响应/已有状态、不留下半个 context/task，终态不倒退，业务数据保持原样；未知状态、审批等待、非终态执行和活跃查询保护父链，回收同步清理别名/反向索引/预留；Response 父链、请求头多值指纹、上游不透明 ID、管理任务旧字段兼容及重启后拒绝旧资源。

自动审批服务曾返回 HTTP 404（审核模型 `gpt-5.6-luna` 不可用），代理未能执行沙箱外测试。随后用户在本机项目目录手动执行以下命令，并提供通过结果：

```sh
GOCACHE=/private/tmp/aiagent-gateway-go-cache go test -race ./...
```

```text
ok      aiagent-gateway/cmd/gateway     (cached)
ok      aiagent-gateway/internal/gateway        3.315s
ok      aiagent-gateway/internal/service        (cached)
```

该结果来自用户提供的终端输出，补齐本机真实 TCP 竞态验证。测试使用本机临时端口、临时目录及模拟上游；SDK/Hermes 互操作和生产容量验证仍待完成。

## P1 配置与统一入口验证（2026-09-30）

本次实现 schema 2、旧配置/管理输入迁移、双接口、统一 `/agents/{appId}` 入口、旧路径别名及独立接口健康。Hermes 适配、A2A 1.0 编解码与官方 SDK/真实 Hermes 联调未完成。

| 检查 | 结果 |
| --- | --- |
| `GATEWAY_TEST_IN_MEMORY=true go test ./...` | 通过，覆盖旧逻辑与新增 P1 测试 |
| `GOCACHE=/private/tmp/aiagent-gateway-go-cache GATEWAY_TEST_IN_MEMORY=true go test -race ./...` | 最终代码通过，三个包均成功 |
| `GOCACHE=/private/tmp/aiagent-gateway-go-cache go vet ./...` | 通过 |
| 本机 `go build -o bin/gateway ./cmd/gateway` | 通过，使用上述临时 GOCACHE |
| `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/aiagent-gateway-linux-amd64 ./cmd/gateway` | 通过，使用上述临时 GOCACHE；未在 Linux 执行 |
| OpenAPI JSON、159 个本地 schema 引用 | 通过；新旧输入分别建模，GET 使用新形态 |
| 文档 JSON 示例、相对链接与 `git diff --check` | 通过 |

新增回归覆盖：schema 1 启动不改文件、原始备份和权限、备份冲突/符号链接拒绝、迁移写失败与重试、ID/版本/旧幂等回执保留、严格区分 schema 1/2、新旧管理输入冲突、接口缺失/版本不一致、嵌套 PATCH 和旧协议更新；统一入口与别名的 URL/查询串/独立凭据、A2A 0.3.0 JSON/SSE 和任务映射、Card 规范 URL、保留路径与关闭入口、接口独立健康及认证探活、共享并发计数、入口间会话隔离、排空/引用保护和配置变更后的过时请求拒绝。未完成的 Hermes/1.0 处理器有 501 拒绝测试。

P1 阶段的真实 TCP 验证曾因沙箱监听限制和自动审批服务 HTTP 404 未能执行；后续已由用户在 P2 阶段手动执行通过，结果见上文。默认 Go 缓存也遇到沙箱访问限制，因此代理执行检查时改用临时目录缓存。Linux 运行及 SDK/Agent 互操作验收仍待完成。

以下保留首版及 systemd 阶段的历史验证记录。

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

- 真实 A2A 0.3.0 SDK/Agent 互操作、任务取消和订阅恢复的完整端到端场景。
- 目标机器 HTTP/SSE 压测：吞吐、p95/p99、连接容量、长期内存稳定性、慢客户端与停机。
- 文件崩溃窗口目前通过注入返回错误检查行为；真实磁盘满、进程 kill 和掉电恢复未验证。

本机 TCP 竞态复核已于 P2 阶段由用户手动执行通过。生产部署前仍需补齐上述验证，不以自动测试作为生产性能承诺。


## 内置 systemd 管理验证

新增服务管理使用二进制子命令和内嵌 unit，无外部安装脚本。

- `GATEWAY_TEST_IN_MEMORY=true go test -race -cover ./...` 通过；网关核心包覆盖率 60.6%；服务管理包另补充首次未加载 unit 和系统账户校验回归测试。
- 服务管理控制流程覆盖首次安装、升级保留凭据和数据、启动失败回滚、重试安装、缺失数据拒绝重建、卸载保留数据、凭据权限及符号链接保护。测试使用临时目录与模拟 systemctl/account 命令。
- 二进制 CLI 冒烟检查通过：`--init-only` 创建/重复验证、损坏配置拒绝覆盖、帮助输出、未知子命令退出码、监听错误非零退出、非法 readiness 地址拒绝。
- `go vet ./...` 和 `git diff --check` 通过。
- Linux amd64 和 arm64 静态交叉构建通过，产物位于 `bin/aiagent-gateway-linux-amd64` 和 `bin/aiagent-gateway-linux-arm64`（构建目录不提交 Git）。

当前主机为 macOS，未修改任何真实 systemd 服务或系统账户。Linux 上仍需验证 unit 加载、EnvironmentFile 解析、服务用户权限、实际启动/重启、开机自启和端口占用失败行为；模拟测试与交叉构建不等于这些环境验证。
