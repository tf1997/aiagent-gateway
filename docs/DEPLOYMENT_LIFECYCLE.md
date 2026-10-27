# 安装、运行文件与升级全流程

更新日期：2026-10-27。安装使用一个完整网关进程，发布期间运行新旧两个网关，正常完成后回到一个进程。入口、业务执行、状态和发布控制一起换版。实现与未完成验收见 [实现进度](SINGLE_PROCESS_IMPLEMENTATION_STATUS.md)。

## 运行方式

Agent 请求、管理 API 和指标共用一个 HTTP listener，由 YAML 的 port 配置，默认 8080。管理 API 和 /metrics 继续要求管理 Token；发布控制使用本机控制 UDS。admin_port 已删除，含该字段的配置会被拒绝。

候选继承布局为一个 HTTP socket、一个控制 UDS、只读配置和私有握手通道，候选协议版本为 3。旧双端口版本与此布局不兼容，不能混合热换版；本轮不自动迁移旧部署。单端口版本之间的连续换版、失败回退和冷启动恢复已验证。

```text
稳定：A（入口、业务、状态 owner、发布控制）
灰度：A（唯一 owner）← 私有 UDS → B（入口、业务、候选视图）
完成：B（入口、业务、状态 owner、发布控制）
服务实例：1 → 2 → 1；可连续 A → B → C
```

systemd 管理 `aiagent-gateway.service`，运行用户为非 root 的 `aiagent-gateway`。`Type=notify`、`NotifyAccess=main`、`KillMode=mixed` 配合 MainPID 通知和 READY 屏障完成父子进程接管。短命 install/status CLI、初始化服务和 systemctl 查询不计入服务实例数。

已在隔离 Debian 12 / systemd 252.39 / Linux arm64 环境验证正式安装、连续换版、失败回退、进程崩溃、stop/start、容器重启、卸载保留数据后重装。其他系统版本与生产负载仍需单独验收。

## 首次安装

```sh
sudo ./gateway-linux-arm64 install
sudo aiagent-gateway rollout status --json
sudo systemctl status aiagent-gateway.service
curl -fsS http://127.0.0.1:8080/readyz
```

安装器需要 `systemctl`、`getent`、`groupadd` 和 `useradd`。root 暂存 SHA-256 命名的只读产物，创建专用账户、管理 CLI、YAML 凭据、部署清单和两个 unit。一次性 init 只在配置存储缺失时运行。已有凭据和业务文件保留，损坏或不完整的既有部署拒绝自动重置。

服务通过 `launch` 启动。它只读交接决定并校验选定产物，随后 exec 到完整运行入口，保持 PID；不启动常驻 supervisor。选定产物缺失、哈希不符或日志损坏时失败，不回退到另一个版本。取得 owner.lock 后再次检查版本选择，解决读取决定与实际取得锁之间的竞态。

## 文件布局

| 路径 | 用途 |
| --- | --- |
| `/usr/local/bin/aiagent-gateway` | 管理 CLI，首装或缺失重装时安装 |
| `/opt/aiagent-gateway/releases/<sha256>/gateway` | root 所有的不可变完整网关产物 |
| `/etc/aiagent-gateway/config.yaml` | 启动配置和管理 Token，0600 |
| `/etc/aiagent-gateway/deployment.json` | `initial_release` 和 `policy` |
| `/etc/systemd/system/aiagent-gateway.service` | 完整网关，ExecStart 调用初始产物的 launch |
| `/etc/systemd/system/aiagent-gateway-init.service` | 一次性路由存储初始化 |
| `/var/lib/aiagent-gateway/owner.lock` | 整个网关的独占写入权，固定 inode |
| `data_dir/handoff/state.log` | 权威交接决定与冷启动选版 |
| `data_dir/handoff/snapshots/` | 有摘要的不可变交接镜像 |
| `data_dir/config.json`、`routes.json`、`route-logs.json` | 配置、路由、幂等回执和审计 |
| `data_dir/shared/`、`entry/`、`release/` | 业务元数据、分配/结果、发布报告 |
| `/run/aiagent-gateway/p/<标识>/` | 候选私有执行与状态通道 |
| `/run/aiagent-gateway-control/controller.sock` | 稳定发布控制 UDS，随 listener FD 交接 |

配置与部署文件通过 LoadCredential 提供给服务；候选通过只读 FD 接收配置。凭据不进入命令行或 status。公共管理 Token 不授予执行产物的权限；发布控制还检查本机 Unix peer UID，正式部署只接受 root。

`initial_release` 仅作为首次启动的种子和长期保留的启动信封。普通升级不改写它；已提交版本由 handoff 决定选择。清理始终保留初始产物、当前决定及发布报告引用的产物、unit 的维护产物和当前快照，另保留最近两个未被引用的完整产物。

## 在线升级

```sh
sudo ./gateway-new install
sudo aiagent-gateway rollout status --json
```

安装器暂存新产物并提交任务，后台控制器独立推进。CLI 等待期间退出不会取消发布；重连后通过 status 查看。相同产物的重复提交复用活动任务；相同健康稳定产物直接成功。有其他活动版本、未排空旧实例或未解决交接时拒绝另一个候选。

默认先将 5% 新逻辑会话分配给候选，旧会话保持亲和。验证通过后全量执行、开启候选公共接入、暂停旧版 Accept，并采集每个必需 profile 的新版直接入口成功样本。旧版本地执行和转发中的长流都必须排空；候选上的活动 SSE 可带租约跨状态提交继续完成。

最终交接捕获并验证完整状态，源关闭所有领域写句柄后让出 owner.lock。候选取得锁、重读决定、提交 COMMITTED，再切为本地状态访问。状态和分配调用在交接窗口有界排队；超时、超容量不会重放业务请求。

`COMPLETED` 要求新版状态/控制独立可用、systemd MainPID 接管已确认、旧进程实际退出、配置解冻。仅收到快照或 HTTP 激活回执不足以完成发布。

## 回退与恢复

```sh
sudo aiagent-gateway rollout rollback
sudo aiagent-gateway rollout resume
```

- 验证期出现有效失败或到期无成功样本：自动恢复旧版流量并排空候选。
- 全量观察缺少新版直接入口样本：`RECOVERY_REQUIRED`，保留两版和证据；补足样本后 resume，或提交前 rollback。
- 源/候选排空超时：保留两版，拒绝第三实例。请求结束后 resume，不强制制造成功。
- 提交回执不确定：重读持久决定、核对身份与锁。COMMITTED 后返回 `ROLLBACK_AFTER_COMMIT`，只能恢复新版或另发兼容产物。
- 新主进程崩溃：systemd 清理服务组后，launch 按持久决定重启；不用旧交接快照覆盖新版随后写入的业务日志。

状态输出保留 rollout/handoff ID、两版身份、决定、阶段时间、实际接管确认的 MainPID、入口与执行样本、状态和候选等待队列、快照大小及交接时间。长交接期间 status 返回最近已持久化的报告，决定与运行状态另行读取；它不是对所有字段的跨进程原子快照。

## 维护、停止与重装

默认 unit 在冷启动前以 root 运行 `cleanup-startup`。只有同时取得安装锁和状态 owner 锁、持久发布已结束且没有待恢复交接时才回收；在线灰度期间不清理、不为清理停止业务。安装器正在启动服务或状态需要恢复时跳过清理并记录原因，保留恢复现场。

清理只处理摘要目录中唯一的、校验通过的 `gateway` 文件，以及不再被当前决定引用的快照和已知日志/快照临时文件。未知文件、带额外文件的产物目录、损坏产物和符号链接保留。清理后同步目录；中途退出最多遗留垃圾，不改写业务日志或发布决定。持续在线期间旧产物仍会累积，可在维护停机后用本轮新版二进制执行 `sudo /path/to/gateway cleanup`；网关仍运行时该命令会拒绝执行。

升级会为原样生成的 unit 补齐或更新维护入口，启动入口仍指向 `initial_release`。定制过的 unit 不自动覆盖；维护入口使用已暂存的新产物，兼容初始启动产物尚不支持 cleanup 的部署。

```sh
sudo aiagent-gateway stop
sudo aiagent-gateway start
sudo aiagent-gateway restart
sudo journalctl -u aiagent-gateway.service -f
sudo aiagent-gateway uninstall
```

停止和重启可能中断连接。systemd 最终清理整个服务 cgroup；普通升级使用 install。YAML 启动参数由本轮进程继承，修改源 YAML 后需维护重启；路由、消费者及上游凭据通过管理 API 更新，发布期间配置写入冻结。

卸载移除服务文件和 CLI，保留配置、业务状态、发布决定、部署清单和产物。重装恢复 unit/CLI 并启动已提交版本，不初始化新的业务基线。

不提供 host/worker 部署角色、`infrastructure`、`--update-host` 或旧拓扑自动迁移。已有旧实验部署切换前，人工停止并核对旧进程退出，取得完整一致备份后重新布置新拓扑；安装器不会自动搬迁或删除用户数据。
