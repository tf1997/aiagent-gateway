# 本机 Hermes 联调环境

使用官方 `nousresearch/hermes-agent:v2026.9.24`，固定镜像索引摘要
`sha256:fca358f12efd65bfaaca05884166f15c0e2788375ca30d77061ac1ebc96452b7`。
Docker Hub 的该标签包含 Linux arm64/amd64，Apple Silicon 使用原生 arm64。
模型服务为 `http://www.tf1997.store:9091`，按 OpenAI 兼容接口使用 `/v1`。

当前状态（2026-10-05）：两个 Hermes 实例及本机网关已启动，`hermes-local` 已升级为 Hermes 专用 HTTP 适配器，A2A 1.0 streaming 已启用。综合 HTTP/SSE/A2A、原生资源故障/排空，以及官方 SDK 的 Card 与六个 RPC 均已通过双实例联调。完整检查项、执行结果和历史 generic 基线见 [验证记录](../../VALIDATION.md)。

## 启动

从项目根目录执行：

```sh
python3 deploy/hermes-local/manage.py up
```

首次启动会隐藏输入模型 API Key，调用 `/v1/models` 列出模型并要求输入准确的模型 ID。
随后会发送一个简短模型请求确认兼容性，生成独立配置和随机的 HTTP/A2A 上游凭据，拉取镜像并启动。
脚本不读取或覆盖宿主机的 `~/.hermes`，也不复用宿主机已有的 `OPENAI_API_KEY`。

已有 Key 文件时可使用（文件只包含 Key）：

```sh
python3 deploy/hermes-local/manage.py up --model '实际模型ID' --api-key-file /绝对路径/model-api-key.txt
```

也可使用环境变量 `HERMES_MODEL_API_KEY`、`HERMES_MODEL_ID`。修改端口需在首次初始化时传入 `--http-port`、`--a2a-port`。重新运行会保留已有配置和凭据；已有配置时不再传 `--model`/`--api-key-file`。

仅生成配置、不启动容器：

```sh
python3 deploy/hermes-local/manage.py configure
```

如果初始化中断导致 `.runtime` 只写入了部分文件，脚本会拒绝覆盖；先检查并另行保存该目录，再重新初始化。镜像启动失败不会删除已生成的配置或会话数据。

## 地址和文件

| 项目 | 默认值 |
| --- | --- |
| HTTP API | `http://127.0.0.1:18642` |
| A2A JSON-RPC | `http://127.0.0.1:19900/` |
| A2A Card | `http://127.0.0.1:19900/.well-known/agent-card.json` |
| Hermes profile | 容器内 default profile |
| 持久化目录 | `deploy/hermes-local/.runtime/home` → `/opt/data` |
| 模型与服务端凭据 | `.runtime/hermes.env` |
| 网关专用凭据 | `.runtime/gateway.env` |
| 网关路由请求 | `.runtime/gateway-route.json` |
| 冒烟结果 | `.runtime/smoke-result.json`，仅检查成功后写入 |

`.runtime` 已加入 Git 忽略；初始化文件为 0600，目录独立于项目源码。Hermes 工具的默认工作目录是容器内 `/opt/data/workspace`，不挂载 Docker socket 或宿主机代码目录。

启动后自动检查 HTTP health、带认证的 capabilities/sessions、Hermes 模型调用、A2A Card 和未知任务 GetTask 错误。模型调用会产生少量模型服务用量。这些检查用于确认部署就绪，不代表 Sessions/Runs/SSE/多 profile 的完整互操作验收。

## 接入本机 aiagent-gateway

已有 Hermes 容器就绪后，在项目根目录执行（需要 Python 3 和 Go 1.24+）：

```sh
python3 deploy/hermes-local/gateway.py up
python3 deploy/hermes-local/check_gateway.py
```

`gateway.py up` 编译并在后台启动独立本机网关，读取 `.runtime/gateway.env` 中的上游凭据，生成随机管理 Token 和两个仅能访问 `hermes-local` 的调用方 Token。它将 `.runtime/gateway-route.json` 注册到管理接口，读取 Hermes Card 后配置 A2A 1.0 JSONRPC 和 streaming 能力，并把模型请求响应头超时设为 180 秒、流空闲超时设为 120 秒、总时长设为 300 秒。

| 项目 | 地址或文件 |
| --- | --- |
| 网关 HTTP 入口 | `http://127.0.0.1:18080/agents/hermes-local`，后接原生 API 路径 |
| 网关 A2A | `http://127.0.0.1:18080/agents/hermes-local/a2a` |
| 网关 Card | `http://127.0.0.1:18080/agents/hermes-local/.well-known/agent-card.json` |
| 管理接口 | `http://127.0.0.1:19090/admin/v1` |
| 本机调用凭据 | `.runtime/gateway/client.env`，包含管理 Token 和主调用方 Token |
| 二进制、日志、路由数据 | `.runtime/gateway/gateway`、`gateway.log`、`data/` |
| 联调结果 | `.runtime/gateway/integration-all.json` |

重复运行 `up` 会复用当前网关与凭据，不重启已运行进程；代码改动后用 `gateway.py restart` 重新编译启动。端口被其他服务占用时拒绝启动；已有端点地址不一致时拒绝覆盖；配置新增的尾部端点可通过管理 API 追加。首次从 generic 升级为 hermes 时先用 restart 清空旧运行时引用，再更新路由类型。`stop` 保留路由数据和凭据，但网关内存中的 session/run/response 和 A2A task/context 映射在重启后失效。

`check_gateway.py` 通过网关发送约五次简短模型请求，创建独立测试会话，并逐项验证：

- Hermes capabilities 和网关 Card 的公开地址、1.0 版本及 streaming 声明。
- 原生 session 创建、同步聊天、第二轮 SSE 回忆第一轮随机代码、历史消息保存。
- Chat Completions SSE 的文本、结束原因和 `[DONE]`。
- A2A 同步 Task、GetTask、同 context 的新任务和 SSE、两页 ListTasks，以及另一调用方的查询隔离。
- 请求结束后实例 `http_inflight` 归零。

报告逐项写入，只有本次全部检查通过才标记 `passed: true`；记录测试 ID、事件类型和耗时，不保存密钥或模型输出。可用 `--only http` / `--only a2a` 分别检查，结果写入对应的 `integration-http.json` / `integration-a2a.json`。测试会话保留在 Hermes 中，运行会产生少量模型用量。

调用示例（加载的是本机网关调用凭据）：

```sh
set -a
. deploy/hermes-local/.runtime/gateway/client.env
set +a
curl --noproxy '*' -fsS \
  -H "Authorization: Bearer $GATEWAY_CALLER_TOKEN" \
  http://127.0.0.1:18080/agents/hermes-local/v1/capabilities
```

HTTP 路由使用 `agent_type: "hermes"`。先 POST `/api/sessions` 创建会话，再保存响应中的 `session.id`；它是公开 ID，不等于创建时的客户端别名。原生资源按路径/正文中的公开 ID 续接，Chat Completions 需显式传已创建会话的 `X-Hermes-Session-Id`。多个引用同时出现时必须一致。A2A 仍要求 `A2A-Version: 1.0`。不同智能体/profile 分别配置独立 route 和上游端点；同一路由的多个端点应是同一智能体的副本。详见 [HTTP 契约](../../docs/HERMES_HTTP_API.md)。

如需接入自行运行的网关，在该进程启动时加载 `.runtime/gateway.env`，通过管理 API 提交 `.runtime/gateway-route.json`；再依据上游 Card 配置路由的 `agent_card.capabilities.streaming` 和模型调用超时。上述 `gateway.py` 管理的是自己的独立网关。

这里的端点用于网关在宿主机运行的情况。若网关也在容器内运行，应使用容器网络服务地址，不能将容器内的 `127.0.0.1` 当成宿主机。

## 双实例和官方 SDK 验证

```sh
python3 deploy/hermes-local/replicas.py up
python3 deploy/hermes-local/gateway.py restart
python3 deploy/hermes-local/check_gateway.py
python3 deploy/hermes-local/check_native.py
python3 -m venv deploy/hermes-local/.runtime/a2a-sdk-venv
deploy/hermes-local/.runtime/a2a-sdk-venv/bin/python -m pip install -r deploy/hermes-local/requirements-a2a-sdk.txt
deploy/hermes-local/.runtime/a2a-sdk-venv/bin/python deploy/hermes-local/check_sdk.py
```

第二个实例使用 `.runtime/home-2` 和独立凭据，端口为 HTTP 18643、A2A 19901，不复制第一个实例的会话数据库。`replicas.py up` 保留已有配置和凭据，追加同一路由的第二个端点；网关 restart 加载新凭据。SDK 本身要求 Python 3.10+；本仓库完整依赖锁定文件在 Python 3.13 验证，建议使用该版本创建环境。

`check_native.py` 验证调用方资源隔离、两实例分配、Runs 幂等/一次性事件订阅边界/steer/stop、Responses 续接、排空和引用保护。它会临时停止第二个测试容器以验证故障不迁移，并在 finally 恢复容器；应与其他模型联调脚本顺序执行。`check_sdk.py` 使用官方 SDK 验证 Card、所有六个 RPC 及两个实例的任务归属，并实际订阅、取消一次测试任务。两者会发送简短模型请求，报告分别为 `.runtime/gateway/integration-native.json` 和 `integration-sdk.json`。

`replicas.py status` 查看两个容器，`replicas.py stop` 只停止第二个。已有 generic 基线报告另存为 `integration-generic-20261004.json`，避免与专用适配器结果混淆。

## 日常操作

```sh
python3 deploy/hermes-local/manage.py status
python3 deploy/hermes-local/manage.py check
python3 deploy/hermes-local/manage.py logs
python3 deploy/hermes-local/manage.py down
python3 deploy/hermes-local/gateway.py status
python3 deploy/hermes-local/gateway.py connect
python3 deploy/hermes-local/gateway.py restart
python3 deploy/hermes-local/gateway.py stop
```

`manage.py down` 使用基础 Compose，仅移除主实例；第二个实例需先用 `replicas.py stop` 停止，存在副本容器时网络可能保留。绑定目录中的会话数据和配置保留。
`connect` 在独立网关运行时重新核对/注册路由；其他网关操作只管理 `.runtime/gateway` 的本机进程。

参考：

- [Hermes 发布 v2026.9.24](https://github.com/NousResearch/hermes-agent/releases/tag/v2026.9.24)
- [官方 Dockerfile](https://github.com/NousResearch/hermes-agent/blob/v2026.9.24/Dockerfile)
- [官方 Compose](https://github.com/NousResearch/hermes-agent/blob/v2026.9.24/docker-compose.yml)
- [镜像标签与架构元数据](https://hub.docker.com/v2/repositories/nousresearch/hermes-agent/tags/v2026.9.24)
