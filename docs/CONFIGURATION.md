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

前台模式更换二进制后重启即可使用新存储。完整网关灰度由当前 owner 维护这些文件，`install` 发布完整程序。控制器从冻结后的配置推导 HTTP、SSE 和 A2A 验收要求。

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

上游 Token 需要原文转发，随路由保存到权限 0600 的 `routes.json`，并通过受认证的私有配置通道传给候选；消费者 Token 仍仅保存哈希。已有 `credential_ref` 必须迁入接口的 `token`，不能将原引用名当成 Token。旧实验部署需在维护窗口人工整理凭据；安装器不自动迁移旧拓扑。

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
| `state_socket` | `/run/aiagent-gateway/state.sock` | 内部状态 Socket 配置；完整候选使用独立私有路径 |
| `entry_socket` | `/run/aiagent-gateway/entry.sock` | 旧入口控制配置；完整服务使用 control_socket |
| `control_socket` | `/run/aiagent-gateway-control/controller.sock` | 发布控制 Socket |
| `deployment` | `/etc/aiagent-gateway/deployment.json` | 部署清单与发布策略 |

`process --config PATH` 是完整网关开发入口；使用部署清单时加 `--deployment PATH --releases-dir PATH --unit UNIT`。正式 unit 使用 `launch` 按持久决定选版后进入 process。候选进程、继承 FD 与握手参数由发布控制器管理。host/worker 角色命令已移除。

## systemd 安装

文件与运行方式见 [部署全流程](DEPLOYMENT_LIFECYCLE.md)。首次 `install` 生成 `/etc/aiagent-gateway/config.yaml`，使用独立随机管理 Token，文件权限 0600，目录权限 0700。全新服务器也可预置有效 YAML，安装器保留原内容。

unit 使用 `LoadCredential=config.yaml:/etc/aiagent-gateway/config.yaml` 和 `--config=%d/config.yaml`。候选经只读 FD 继承本轮配置；修改源 YAML 后需维护重启才能生效。路由、消费者及上游 Token 通过管理 API 动态更新，发布期间冻结配置写入。

部署清单只包含 `initial_release` 和 `policy`。初始产物作为 launch 启动信封长期保留；已提交产物由 handoff 决定选定。当前已在 Debian 12 / systemd 252.39 验证正式安装和进程接管，其他系统版本未作支持承诺。

安装器生成的 `data_dir` 为 `/var/lib/aiagent-gateway`，使用默认的绝对 Socket、部署清单和发布日志路径；不兼容路径在启动/升级前拒绝。监听端口、公开 URL 和容量可调整。自定义内部路径应使用独立开发配置或自行维护 unit，不与既有服务共用数据目录。

普通 `install` 发布完整网关，没有 `--update-host`。旧实验拓扑不自动迁移，不增加部署版本字段，不自动清空数据。需要迁移时，先人工停止并核对所有旧进程退出，取得包含配置、共享状态、发布决定和凭据的一致备份，再部署新拓扑。
