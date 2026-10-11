# YAML 启动配置

网关现在只从 YAML 文件读取运行配置。原 `GATEWAY_*` 启动环境变量已移除，不参与覆盖，也不作为缺失配置的回退；上游凭据同样不再从环境变量读取。路由仍通过管理 API 配置，并保存到数据目录的 `config.json`。

## 快速启动

```sh
cp gateway.example.yaml gateway.yaml
chmod 600 gateway.yaml
# 编辑 gateway.yaml，替换管理 Token 和上游凭据。
go build -o bin/aiagent-gateway ./cmd/gateway
./bin/aiagent-gateway --config ./gateway.yaml --init-only
./bin/aiagent-gateway --config ./gateway.yaml
```

不传 `--config` 时读取当前工作目录的 `gateway.yaml`。配置文件不存在、字段未知/重复、类型不正确、显式 null、多个 YAML 文档或容量非正数都会拒绝启动；解析错误不回显配置值。文件最大 1 MiB。

`--init-only` 只初始化或校验路由存储；`--init` 允许首次初始化后继续监听。两者都不会覆盖损坏的路由存储，常规启动省略这两个选项。`--check-ready` 从同一 YAML 读取监听地址执行就绪检查。

最小实用配置：

```yaml
listen: "127.0.0.1:8080"
admin_listen: "127.0.0.1:9090"
data_dir: "./data"
public_url: "http://127.0.0.1:8080"
admin_token: "replace-with-admin-secret"
credentials:
  hermes_http: "replace-with-http-secret"
  hermes_a2a: "replace-with-a2a-secret"
```

YAML 只保存管理 Token 和上游凭据，**不再接受 callers 字段**。消费者通过路由管理 API 的 `consumers` 动态配置，见 [使用说明](USAGE.md#动态配置消费者)。Token 建议加引号；内容按字面读取，不展开 `$VAR` / `${VAR}`。没有上游认证时可省略 `credentials` 或设置 `{}`。

相对数据目录、Socket 和其他文件路径都相对于 **YAML 文件所在目录**，不随进程工作目录变化。修改 YAML 后需重启相应进程；路由管理 API 的更新仍立即生效。

## 上游凭据

Endpoint 接口中的 `credential_ref` 改为引用 YAML `credentials` 的键：

```json
{
  "key": "assistant",
  "agent_type": "hermes",
  "expose": ["proxy", "a2a"],
  "consumers": [{"id":"app-a","token":"replace-with-caller-secret"}],
  "endpoints": [{
    "name": "hermes-1",
    "interfaces": {
      "proxy": {"url":"http://127.0.0.1:18642","credential_ref":"hermes_http"},
      "a2a": {"url":"http://127.0.0.1:19900/","protocol_version":"1.0","credential_ref":"hermes_a2a"}
    }
  }]
}
```

网关注入 `Authorization: Bearer <值>`，凭据值不要包含 `Bearer ` 前缀。HTTP、A2A、健康检查、能力探测和后台核查共用此字典。配置加载和管理写入都会校验引用存在；仅在环境变量中提供同名值不会通过校验。

已有路由无需重建：将旧 `credential_ref` 对应的环境变量名直接作为字典键，填入原 Token 即可。凭据不会进入路由 `config.json`、管理响应或共享状态日志。

## 字段与默认值

| YAML 字段 | 默认值 | 用途 |
| --- | --- | --- |
| `admin_token` | 必填 | 管理及本机控制认证 |
| `credentials` | 空字典 | 上游 Bearer Token 引用 |
| `listen` | `:8080` | 数据监听 |
| `admin_listen` | `127.0.0.1:9090` | 管理监听 |
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

`entry`、`state-server`、`worker`、`rollout-controller` 都接受角色名后的 `--config PATH`。worker 的 `--slot`、`--release`、`--listen` 和 `--admin` 是发布控制器传入的实例参数；显式监听参数覆盖该角色 YAML 的监听地址。

旧环境变量到 YAML 的对应关系：去掉 `GATEWAY_` 前缀并改为小写，例如 `GATEWAY_ADMIN_TOKEN` → `admin_token`、`GATEWAY_MAX_INFLIGHT` → `max_inflight`。`GATEWAY_CALLERS` 不迁入 YAML，应为每个路由通过管理 API 登记 `consumers`；`GATEWAY_INIT` 改为一次性 CLI 选项；上游 Token 放进 `credentials`。

## systemd 安装

首次 `install` 生成 `/etc/aiagent-gateway/gateway.yaml`，使用独立随机 Token，文件权限 0600，目录权限 0700。也可在全新服务器预先放置有效 YAML；安装器保留已有内容。

服务 unit 使用 `LoadCredential=gateway.yaml:/etc/aiagent-gateway/gateway.yaml` 和 `--config=%d/gateway.yaml`，由 systemd 将 root 文件提供给服务用户读取。需要支持该机制的 systemd（247+），详情见 [systemd 凭据文档](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html#LoadCredential=)。不再使用 EnvironmentFile，也不需要放宽原文件权限。

安装器生成的 `data_dir` 是 `/var/lib/aiagent-gateway`。内置 systemd unit/driver 使用固定的状态目录、Socket、部署清单及发布日志路径，预置 YAML 也必须使用上述绝对默认路径；不兼容的路径配置会在启动/升级前拒绝。监听地址、公开 URL、上游凭据和容量可调整。消费者凭据通过路由 API 动态调整，无需重启。需要自定义内部路径时，使用前台角色启动或自行维护 unit。

### 从环境变量版本迁移

这是进程配置格式变更，旧二进制不能读取新的 YAML；新版也不会 source 旧 `gateway.env`。先将原地址、管理 Token 和上游凭据转入 YAML，保留原文件及二进制备份。删除 YAML 中的 `callers`，新进程启动后按原 routes 权限为每条路由提交 `consumers:[{"id":"原调用方ID","token":"原调用方Token"}]`。迁移后续接资源必须保留原调用方 ID、路由文件、Endpoint ID 和上游凭据；没有消费者的旧路由会拒绝业务请求。此认证配置变更需基础组件与 worker 一起维护升级，不应混跑旧的静态认证进程。

- 前台单进程：停止旧进程，用新二进制和 `--config` 启动；原进程内资源映射仍会丢失。
- 旧单进程 systemd：先保存 YAML，再在维护窗口执行新二进制的 `install --migrate`。
- 已有灰度部署：旧入口、状态服务、发布控制器及 worker 都读取环境变量，需一起维护迁移。普通 install 会拒绝旧部署格式，不能只升级 worker。

已有灰度部署在维护窗口迁移时，先结束正在进行的发布、暂停流量并确认请求/后台执行结束，然后保存整套文件和状态备份。停止服务组后，用旧管理 CLI 卸载 unit（它保留数据）；将旧 `deployment.json`、入口状态目录 `/var/lib/aiagent-gateway/entry` 和发布日志目录 `/var/lib/aiagent-gateway-rollout` 移入独立备份目录。保留 `/var/lib/aiagent-gateway/config.json` 和 `/var/lib/aiagent-gateway/shared` 原位，再执行新二进制 `install --migrate`，重新建立 YAML 版入口和发布基线。不要将旧控制日志恢复到运行中的新部署。

这一步会中断连接、重建发布基线，旧 release 仍保留用于维护恢复，但环境变量版本不能作为新版控制器的日常灰度候选。恢复旧部署需要停机并整体恢复先前备份，不能用旧配置覆盖新版已接收的资源数据。当前仍需 Linux/systemd 实机验收。

本机 Hermes 工具将管理 Token 和上游凭据写入 `.runtime/gateway/gateway.yaml`，显式传 `--config` 启动，再通过路由 POST 登记保存的消费者。`client.env` 仅供 curl 客户端使用，不再是网关启动配置。
