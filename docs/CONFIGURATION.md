# YAML 启动配置

网关现在只从 YAML 文件读取运行配置。原 `GATEWAY_*` 启动环境变量已移除，不参与覆盖，也不作为缺失配置的回退；上游凭据同样不再从环境变量读取。路由仍通过管理 API 配置，并保存到数据目录的 `routes.json`；管理幂等回执和审计记录保存在 `route-logs.json`，`config.json` 只保留版本信息。

## 快速启动

```sh
cp config.example.yaml config.yaml
chmod 600 config.yaml
# 编辑 config.yaml，替换管理 Token。
go build -o bin/aiagent-gateway ./cmd/gateway
./bin/aiagent-gateway --config ./config.yaml
```

不传 `--config` 时读取当前工作目录的 `config.yaml`。配置文件不存在、字段未知/重复、类型不正确、显式 null、多个 YAML 文档或容量非正数都会拒绝启动；解析错误不回显配置值。文件最大 1 MiB。

启动时自动判断路由存储：数据目录中三份文件均不存在时创建初始存储，存在则读取并校验；拆分文件缺失、损坏、不可读或版本不一致时拒绝启动，不自动清空。首次启动无需额外步骤，`--init` 参数已移除。`--init-only` 保留为可选的初始化或校验后退出命令，不监听端口；`--check-ready` 从同一 YAML 读取监听地址执行就绪检查。

## 路由存储文件

| 文件 | 内容 |
| --- | --- |
| `config.json` | `storage_version: 1`、`schema_version: 2` 和 `config_revision` |
| `routes.json` | 相同版本信息及路由列表，内嵌 Endpoint、上游 Token 和消费者 Token 哈希 |
| `route-logs.json` | 相同版本信息及 `idempotency`、`idempotency_order`、`audit`；幂等回执和审计记录各保留最近 1000 条 |

三份文件由管理 API 一起提交，权限为 0600。`route-logs.json` 保存现有管理调用记录，不记录业务请求正文。写入期间的 `.config.pending.json` 是恢复事务；启动时先完成该事务，再读取三份文件。不要删除未完成的事务文件，也不要直接编辑运行中的存储。

旧版将所有内容放在 `config.json` 的 schema 1/2 文件仍可读取。启动和幂等重放不改写旧文件；首次成功管理修改前，先原样保存为 `config.schema-1.backup.json` 或 `config.schema-2.backup.json`，再拆分写入，保留路由/Endpoint ID、凭据、幂等回执和审计历史。同名备份内容冲突时拒绝迁移。旧单文件与拆分文件混放时拒绝启动，避免不完整恢复丢失路由。备份或恢复应在停止写入后覆盖整个数据目录，不能只复制 `config.json`；旧二进制不支持拆分格式。

前台模式更换二进制后重启即可使用新存储。灰度部署由 `state-server` 维护这些文件，需要按基础组件维护流程更新状态服务；日常 `install` 只发布 worker。新版安装器能从新旧存储推断 HTTP、SSE 和 A2A 验收要求。

最小实用配置：

```yaml
port: 8080
admin_port: 9090
data_dir: "./data"
public_url: "http://127.0.0.1:8080"
admin_token: "replace-with-admin-secret"
```

`port` 和 `admin_port` 只填写 1–65535 的端口号，支持数字或带引号的数字，不填写 IP 或冒号。数据面和公共管理面均监听所有网卡，默认地址分别为 `:8080`、`:9090`。远程管理访问 `http://<网关IP或域名>:9090/admin/v1/...`，请求携带 `Authorization: Bearer <admin_token>`；自定义 `admin_port` 时使用对应端口。

YAML 保存管理 Token 和进程设置，**不接受 `callers` 或 `credentials` 字段**。消费者通过路由管理 API 的 `consumers` 动态配置，上游 Token 放在路由各个接口的 `token` 中，见 [使用说明](USAGE.md#动态配置消费者)。Token 按字面读取，不展开 `$VAR` / `${VAR}`。

相对数据目录、Socket 和其他文件路径都相对于 **YAML 文件所在目录**，不随进程工作目录变化。修改 YAML 后需重启相应进程；路由管理 API 的更新仍立即生效。

## 上游凭据

上游 Agent 的 Bearer Token 直接配置在路由的 `endpoints[].interfaces.proxy.token` 或 `endpoints[].interfaces.a2a.token` 中：

```json
{
  "key": "assistant",
  "agent_type": "hermes",
  "expose": ["proxy", "a2a"],
  "consumers": [{"id":"app-a","token":"replace-with-caller-secret"}],
  "endpoints": [{
    "name": "hermes-1",
    "interfaces": {
      "proxy": {"url":"http://127.0.0.1:18642","token":"replace-with-http-secret"},
      "a2a": {"url":"http://127.0.0.1:19900/","protocol_version":"1.0","token":"replace-with-a2a-secret"}
    }
  }]
}
```

网关注入 `Authorization: Bearer <token>`，Token 不要包含 `Bearer ` 前缀。HTTP、A2A、健康检查、能力探测和后台核查都使用对应接口的 Token。管理写入和配置加载校验长度（最多 4096 字节）及换行/NUL；不读取 YAML 凭据字典或环境变量。

更新 Token 通过路由 POST/upsert 或 Endpoint PATCH 动态生效，无需重启。更新已有接口时省略 `token` 保留原值，显式 `null` 或空字符串移除认证；首次创建省略表示无需上游认证。管理响应及幂等回执不返回 Token。

上游 Token 需要原文转发，随路由保存到权限 0600 的 `routes.json`，并通过受认证的本机共享配置传给 worker；消费者 Token 仍仅保存哈希。已有 `credential_ref` 必须迁入接口的 `token`，不能将原引用名当成 Token。迁移步骤见下文。

## 字段与默认值

| YAML 字段 | 默认值 | 用途 |
| --- | --- | --- |
| `admin_token` | 必填 | 管理及本机控制认证 |
| `port` | `8080` | 数据端口，监听所有网卡 |
| `admin_port` | `9090` | 公共管理端口，监听所有网卡，支持远程访问 |
| `data_dir` | `./data` | 路由与共享状态目录 |
| `public_url` | 空 | A2A Card 对外基础地址；使用 Card 时必须设置 |
| `max_inflight` | 4096 | 全局并发上限 |
| `max_per_caller` | 1024 | 每调用方并发上限 |
| `max_per_endpoint` | 256 | 每实例并发上限 |
| `max_sessions` | 100000 | 会话/context 容量 |
| `max_tasks` | 100000 | Run/Task 容量 |
| `max_resources` | 200000 | 公共资源总容量 |
| `max_resource_aliases` | 200000 | 资源别名容量 |
| `max_data_idempotency` | 100000 | 数据幂等预留容量 |
| `state_socket` | `/run/aiagent-gateway/state.sock` | 状态服务 Socket |
| `entry_socket` | `/run/aiagent-gateway/entry.sock` | 入口控制 Socket |
| `control_socket` | `/run/aiagent-gateway-control/controller.sock` | 发布控制 Socket |
| `deployment` | `/etc/aiagent-gateway/deployment.json` | 部署清单与发布策略 |
| `rollout_data` | `/var/lib/aiagent-gateway-rollout` | 发布日志目录 |

`entry`、`state-server`、`worker`、`rollout-controller` 都接受角色名后的 `--config PATH`。worker 的 `--slot`、`--release`、`--listen` 和 `--admin` 是发布控制器传入的实例参数；显式监听参数仍使用 `host:port`，覆盖该角色由 YAML 端口生成的监听地址。

旧环境变量到 YAML 的对应关系：去掉 `GATEWAY_` 前缀并改为小写，例如 `GATEWAY_ADMIN_TOKEN` → `admin_token`、`GATEWAY_MAX_INFLIGHT` → `max_inflight`。`GATEWAY_LISTEN` 和 `GATEWAY_ADMIN_LISTEN` 分别迁入 `port` 和 `admin_port`，只保留端口号。`GATEWAY_CALLERS` 不迁入 YAML，应为每个路由通过管理 API 登记 `consumers`；`GATEWAY_INIT` 改为一次性 CLI 选项；上游 Token 放进路由接口的 `token`。

## systemd 安装

安装生成的文件、各角色读取配置的方式，以及全组件维护步骤见 [安装、运行文件与升级全流程](DEPLOYMENT_LIFECYCLE.md)。

首次 `install` 生成 `/etc/aiagent-gateway/config.yaml`，使用独立随机 Token，文件权限 0600，目录权限 0700。也可在全新服务器预先放置有效 YAML；安装器保留已有内容。

服务 unit 使用 `LoadCredential=config.yaml:/etc/aiagent-gateway/config.yaml` 和 `--config=%d/config.yaml`，由 systemd 将 root 文件提供给服务用户读取。需要支持该机制的 systemd（247+），详情见 [systemd 凭据文档](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#LoadCredential=)。不再使用 EnvironmentFile，也不需要放宽原文件权限。

安装器生成的 `data_dir` 是 `/var/lib/aiagent-gateway`。内置 systemd unit/driver 使用固定的状态目录、Socket、部署清单及发布日志路径，预置 YAML 也必须使用上述绝对默认路径；不兼容的路径配置会在启动/升级前拒绝。监听端口、公开 URL 和容量可调整。消费者及上游凭据通过路由 API 动态调整，无需重启。需要自定义内部路径时，使用前台角色启动或自行维护 unit。

### 从 gateway.yaml / credentials 版本迁移

在维护窗口停止网关并备份启动配置、路由文件和服务 unit。将原 `gateway.yaml` 改名为 `config.yaml`，将原 `listen`、`admin_listen` 分别改名为 `port`、`admin_port`，值仅保留端口号；对 `config.json` 的每个接口，将原 `credential_ref` 在旧 YAML `credentials` 中对应的值写入 `token`，删除 `credential_ref`，再删除 YAML 的整个 `credentials` 字段。保留管理 Token、路由 ID、Endpoint ID、版本及消费者数据。启动配置和路由文件均保持 0600。

前台进程更新 `--config` 路径后启动。systemd 部署需同时替换所有基础组件及 worker 的二进制和 unit，将 `LoadCredential` 和 `--config` 改为本文的 `config.yaml` 路径；普通 worker 灰度升级会拒绝仍使用 `gateway.yaml` 的基础组件，避免混用两种上游认证配置。启动前可用 `--config ... --init-only` 校验现有路由文件。旧的 `credential_ref` 会被拒绝，不会静默丢弃认证。

### 从环境变量版本迁移

这是进程配置格式变更，旧二进制不能读取新的 YAML；新版也不会 source 旧 `gateway.env`。先将原地址和管理 Token 转入 YAML，将上游凭据填入路由接口的 `token` 并删除 `credential_ref`，保留原文件及二进制备份。删除 YAML 中的 `callers`，新进程启动后按原 routes 权限为每条路由提交 `consumers:[{"id":"原调用方ID","token":"原调用方Token"}]`。迁移后续接资源必须保留原调用方 ID、路由文件、Endpoint ID 和上游凭据；没有消费者的旧路由会拒绝业务请求。此认证配置变更需基础组件与 worker 一起维护升级，不应混跑旧的静态认证进程。

- 前台单进程：停止旧进程，用新二进制和 `--config` 启动；原进程内资源映射仍会丢失。
- 旧单进程 systemd：先保存 YAML，再在维护窗口执行新二进制的 `install --migrate`。
- 已有灰度部署：旧入口、状态服务、发布控制器及 worker 都读取环境变量，需一起维护迁移。普通 install 会拒绝旧部署格式，不能只升级 worker。

已有灰度部署在维护窗口迁移时，先结束正在进行的发布、暂停流量并确认请求/后台执行结束。停止服务组，核对所有子进程退出后，保存整套文件和状态备份。保留服务组主 unit，供安装器在重建前再次执行 stop；若旧 state unit 仍引用 `EnvironmentFile=` 或 `gateway.yaml`，将该 unit 归档移出 `/etc/systemd/system`，由安装器重建。将旧 `deployment.json`、入口状态目录 `/var/lib/aiagent-gateway/entry` 和发布日志目录 `/var/lib/aiagent-gateway-rollout` 移入独立备份目录。保留转换后的 `config.yaml`、`/var/lib/aiagent-gateway` 下的整套路由存储和 `/var/lib/aiagent-gateway/shared` 原位，再执行新二进制 `install --migrate`，重新建立 YAML 版入口和发布基线。具体备份与重建顺序见 [整套组件更新](DEPLOYMENT_LIFECYCLE.md#整套组件更新)；旧控制日志仅用于维护恢复，不写回正在运行的新部署。

这一步会中断连接、重建发布基线，旧 release 仍保留用于维护恢复，但环境变量版本不能作为新版控制器的日常灰度候选。恢复旧部署需要停机并整体恢复先前备份，不能用旧配置覆盖新版已接收的资源数据。当前仍需 Linux/systemd 实机验收。

本机 Hermes 工具将管理 Token 写入 `.runtime/gateway/config.yaml`，显式传 `--config` 启动，再通过路由 POST 登记上游 Token 和消费者。`client.env` 仅供 curl 客户端使用，不再是网关启动配置。
