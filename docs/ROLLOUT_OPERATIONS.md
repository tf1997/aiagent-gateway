# 灰度发布操作

更新日期：2026-10-23。完整网关按 1 → 2 → 1 发布，入口、业务、状态和发布控制一起升级。正式安装和 Linux/systemd 生命周期已验证；实际 Agent 完整换版、目标生产容量和存储/系统矩阵未完成。详见 [实现进度](SINGLE_PROCESS_IMPLEMENTATION_STATUS.md) 和 [部署全流程](DEPLOYMENT_LIFECYCLE.md)。

## 构建发版

以下命令供后续发版执行，本文更新未执行远程安装或发布。构建机需要 Go 1.24+、Git 和 tar；目标服务器使用 Linux/systemd，无须安装 Go。

从要发布的已提交代码构建，先完成回归、竞态和静态检查，再打包两个 Linux 架构。示例版本标签 `v2026.10.23.1` 用于文件归档，可按团队规则替换；网关真正使用的 `release` 是二进制 SHA-256，不是该标签。当前没有 `--version` 或 `install --version` 参数。

在仓库根目录执行下面整个代码块。子 shell 中任何一步失败都会停止，避免继续打包失败产物；`bin/` 已在 Git 忽略规则中，同名目录存在时拒绝覆盖。

```sh
(
    set -eu
    release_label=v2026.10.23.1
    release_dir="bin/releases/${release_label}"
    test -z "$(git status --porcelain)"
    test ! -e "$release_dir"

    go test ./...
    go test -race ./...
    go vet ./...

    mkdir -p "$release_dir"
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
        -o "$release_dir/aiagent-gateway-linux-amd64" ./cmd/gateway
    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath \
        -o "$release_dir/aiagent-gateway-linux-arm64" ./cmd/gateway
    {
        printf 'release_label=%s\n' "$release_label"
        printf 'git_commit=%s\n' "$(git rev-parse HEAD)"
        go version
    } > "$release_dir/BUILD-INFO.txt"
    (
        cd "$release_dir"
        if command -v sha256sum >/dev/null 2>&1; then
            sha256sum aiagent-gateway-linux-amd64 aiagent-gateway-linux-arm64 BUILD-INFO.txt > SHA256SUMS
        else
            shasum -a 256 aiagent-gateway-linux-amd64 aiagent-gateway-linux-arm64 BUILD-INFO.txt > SHA256SUMS
        fi
    )
    tar -czf "bin/releases/${release_label}.tar.gz" -C bin/releases "$release_label"
)
```

产物为 `bin/releases/v2026.10.23.1.tar.gz`，内含 amd64/arm64 二进制、构建信息和校验文件。保留压缩包、源码 commit、测试记录及发布说明；发布说明应注明配置/状态兼容性、涉及的代理协议和恢复方式。CI 可归档这些文件，当前仓库没有自动发布到制品库的流程。

发布前还需在 Linux 测试环境使用该产物验收安装和灰度链路。本机多进程复现命令见文末，不能代替目标系统及真实 Agent 验收。

## 安装与升级

在已验收的 Linux/systemd 环境上传对应架构产物。首次安装和后续升级使用同一命令：

```sh
sudo ./aiagent-gateway-linux-amd64 install
sudo aiagent-gateway rollout status --json
sudo systemctl status aiagent-gateway.service
```

首次安装创建一个完整服务实例。后续 install 暂存产物并提交灰度任务，CLI 中断不取消任务。相同产物复用原任务，另一个产物在当前任务或排空未解决时被拒绝。系统管理 CLI 保留首装版本，升级时使用新上传二进制的 install。

部署清单只有 `initial_release` 和 `policy`。不包含 infrastructure 或拓扑版本；不使用 --update-host。不要覆盖运行中的归档产物、修改日志来选版或恢复旧快照覆盖当前数据。

## 默认验证规则

| 阶段 | 要求 |
| --- | --- |
| readiness | 30 秒内完成候选身份、协议与配置版本核对 |
| VERIFYING | 5% 新逻辑会话；至少 60 秒、最长 300 秒；每必需 profile 至少 10 次成功；有效失败阈值 0 |
| OBSERVING | 全量后至少 300 秒；每 profile 至少 10 次新成功，且新版直接入口成功样本达标 |
| DRAINING | 等旧入口、旧执行及转发排空，期限 600 秒 |
| HANDOFF / ACTIVATING | 完整镜像与差量验证、写锁交接、COMMITTED、控制 listener 和 MainPID/READY 确认 |
| COMPLETED | 新版独立运行、旧 PID 已退出、配置解冻 |

profiles 包括 http、sse、a2a、a2a_sse。开启 auto_profiles 时，在配置冻结后从 owner 配置推导，并纳入观察到的业务类型。健康检查、Card 和未访问 Agent 的响应不算成功；HTTP 200 内的协议错误、SSE 首帧后断流不能补成成功样本。

交接 Policy 的等待时间、队列数量/字节、快照和差量上限见 [实施方案](SINGLE_PROCESS_ROLLOUT_PLAN.md#64-初始时间与容量预算)。缺省值由服务明确补齐；临时传输限额不会覆盖接管后的长期 Policy。

## 状态、回退和恢复

```sh
sudo aiagent-gateway rollout status --json
sudo aiagent-gateway rollout rollback
sudo aiagent-gateway rollout resume
sudo journalctl -u aiagent-gateway.service -f
```

| 状态或原因 | 处理 |
| --- | --- |
| VERIFYING 样本不足到期 | 自动回退；流量不足不是成功 |
| direct_ingress_observation_incomplete | 保留两版与样本，补充新版入口真实请求后 resume，或提交前 rollback |
| source_drain_timeout / candidate_drain_timeout | 等待在途请求结束再 resume；保留两版且不允许第三实例 |
| ROLLBACK_AFTER_COMMIT | 已经提交新版，不能恢复旧写入者；恢复当前版本或发布另一个兼容产物 |
| 存储不可用、日志损坏、选定产物缺失 | 修复原因并按持久决定恢复；不能跳过检查或手工改写选版 |

成功需确认 `phase == COMPLETED`、`stable.release` 是本次产物摘要、`committed` 和 handoff 决定相符、MainPID 是新版，并核对 readiness 和真实业务。首装没有 handoff，committed 为 false 是正常情况。进程重启后的终态可以保留之前的 ROLLED_BACK，稳定版本仍由持久决定选定。

`state_queue` 与 `allocation_queue` 的 `PauseCount`、`LastPauseMicros`、`MaxPauseMicros`、`TotalPauseMicros` 记录候选各屏障从关闭接纳到重新开放的实际时长，包含已接纳调用的排空时间；`CurrentPauseMicros` 表示尚未结束的暂停。单位为微秒，计数属于当前屏障实例，重启后不恢复。`handoff_milliseconds` 还包含预览传输等过程，不能当作业务暂停时长。评估负载时同时检查客户端错误、`Timeouts` 和 `Rejected`，不能仅看发布是否 COMPLETED。

现有 SSE 不迁移 TCP 连接：旧版流排空后退出，新版活动流可跨状态 owner 交接继续。业务请求不会因失败而改版重放。已有会话、公开资源 ID 和 Agent Endpoint 归属保留，冷启动不能恢复已经断开的 TCP/SSE。

## 验证命令

```sh
go test ./...
go test -race ./...
go vet ./...
go build -o /tmp/gateway ./cmd/gateway
python3 -B deploy/rollout-process/complete.py /tmp/gateway
python3 -B deploy/rollout-systemd/process.py --reuse-image --cycles 20
python3 -B deploy/rollout-process/load.py --concurrency 100
python3 -B deploy/rollout-process/agent.py /tmp/gateway
```

systemd 脚本使用隔离容器，无宿主目录挂载与公开端口，完成后清理。`--reuse-image` 使用已有 `aiagent-gateway-systemd-test:bookworm`；不传该项时构建镜像。测量报告默认 `/tmp/gateway-systemd-process.json`，是小状态、串行 HTTP 回归负载，不是生产容量上限。

并发脚本复用上述 Linux 镜像，自动构建三份实际网关产物，以非 root 用户在仅有 loopback 的隔离容器中执行。HTTP 与 SSE 两阶段各保持指定数量的闭环客户端，每次请求使用新会话，分别完成 A→B、B→C；模拟上游耗时为 HTTP 50 ms、SSE 250 ms。默认报告 `/tmp/gateway-concurrent-rollout.json` 包含延迟、完整 SSE 校验、上游重复调用数、队列/屏障、合计 CPU/RSS/FD 与进程数；失败也保存报告并以非零状态退出。测试使用缩短的观察时间，但保留默认 2 秒状态等待和 5 秒最终屏障预算。CPU 为每核 100%，资源按约 100 ms 采样，可能漏掉采样间隙的短峰；容器共享开发机资源，结果不能外推为生产容量上限。

实际 Agent 脚本使用本机已有的模型配置，创建两个临时 Hermes 容器与独立网关，发送少量合成模型请求。报告默认 `/tmp/gateway-real-agent-rollout.json`，仅 passed=true 才表示完整换版通过；上游过载时不修改断言来凑成功。
