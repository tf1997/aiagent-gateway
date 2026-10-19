# 自动灰度升级操作说明

状态：新安装默认使用 host-worker（2 → 3 → 2），已通过真实进程连续发布、SSE 排空、回退与崩溃恢复测试；Linux systemd 实机部署、真实模型升级联调及生产压力验收尚未完成。以下命令是当前实现的接口。

组件改造、会话到原 Agent 的映射持久化、代码位置及测试范围见 [实现过程记录](ROLLOUT_IMPLEMENTATION_RECORD.md)。

首次安装、配置与运行文件、进程交接的完整解释见 [安装与升级全流程](DEPLOYMENT_LIFECYCLE.md)；入口、管理/状态服务、控制器和 CLI 一起更新时，见其中的 [整套组件更新](DEPLOYMENT_LIFECYCLE.md#整套组件更新)。

操作顺序：[构建发版](#构建发版) → [上传与安装前检查](#上传与安装前检查) → [首次安装或宿主更新](#首次安装或宿主更新) / [日常灰度升级](#日常灰度升级) → [结果确认和异常处理](#结果确认和异常处理)。

## 构建发版

以下命令供后续发版执行，本文更新未执行远程安装或发布。构建机需要 Go 1.24+、Git 和 tar；目标服务器使用 Linux/systemd，无须安装 Go。

从要发布的已提交代码构建，先完成回归、竞态和静态检查，再打包两个 Linux 架构。示例版本标签 `v2026.10.06.1` 用于文件归档，可按团队规则替换；网关真正使用的 `release` 是二进制 SHA-256，不是该标签。当前没有 `--version` 或 `install --version` 参数。

在仓库根目录执行下面整个代码块。子 shell 中任何一步失败都会停止，避免继续打包失败产物；`bin/` 已在 Git 忽略规则中，同名目录存在时拒绝覆盖。

```sh
(
    set -eu
    release_label=v2026.10.06.1
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

产物为 `bin/releases/v2026.10.06.1.tar.gz`，内含 amd64/arm64 二进制、构建信息和校验文件。保留压缩包、源码 commit、测试记录及发布说明；发布说明应注明配置/状态兼容性、涉及的代理协议和恢复方式。CI 可归档这些文件，当前仓库没有自动发布到制品库的流程。

发布前还需在 Linux 测试环境使用该产物验收安装和灰度链路。本机多进程复现命令见文末，不能代替目标系统及真实 Agent 验收。

## 上传与安装前检查

在构建机上传产物，替换示例账号和主机名：

```sh
scp bin/releases/v2026.10.06.1.tar.gz deploy@gateway-host:/tmp/
```

登录目标服务器，在新目录解压并校验；校验失败应停止，重新核对或传输产物。以下目录名与上一节版本标签对应：

```sh
mkdir /tmp/aiagent-release-v2026.10.06.1
tar -xzf /tmp/v2026.10.06.1.tar.gz -C /tmp/aiagent-release-v2026.10.06.1
cd /tmp/aiagent-release-v2026.10.06.1/v2026.10.06.1
sha256sum -c SHA256SUMS
uname -m
```

`x86_64` 使用 `aiagent-gateway-linux-amd64`；`aarch64`/`arm64` 使用 `aiagent-gateway-linux-arm64`。下文均以 amd64 为例，ARM64 服务器替换对应文件名。重试上传或解压时使用新目录，避免混入上次产物。

安装前确认：

- 服务器运行 systemd 247+（支持 LoadCredential），具备 `systemctl`、`getent`、`groupadd`、`useradd`，操作账号可使用 sudo。
- 首次安装时公共数据/管理端口可用。worker 使用动态 loopback 端口。可用 `sudo ss -ltnp` 核对监听进程。
- 已有灰度部署的 `rollout status --json` 可读取，当前没有未结束发布；处于 `RECOVERY_REQUIRED` 时先处理该任务。
- 新版兼容当前基础组件的控制/状态协议，且灰度中新建资源仍可被旧版读取。需要升级基础组件或变更不兼容状态格式时，安排独立维护迁移。
- 灰度窗口内有足够的新会话和相应 HTTP/SSE/A2A 请求。只有存量会话或没有流量，可能无法取得候选成功样本；先确定发布策略，不在验证中修改配置。

## 首次安装或宿主更新

程序从 config.yaml 读取启动配置。上游 Token 位于路由接口的 token，消费者随路由 POST 的 consumers 动态写入。部署清单只维护基础产物路径、初始业务产物路径和发布策略。

### 全新服务器

在已解压并校验的目录执行：

```sh
chmod +x ./aiagent-gateway-linux-amd64
sudo ./aiagent-gateway-linux-amd64 install
sudo aiagent-gateway rollout status --json
sudo systemctl list-units --all 'aiagent-gateway*'
curl --fail --silent --show-error http://127.0.0.1:8080/readyz
```

安装器创建系统用户、凭据、配置、部署清单和单个 host service，由 host 启动初始 worker，并启用开机启动。管理凭据保存在 `/etc/aiagent-gateway/config.yaml`，不输出到终端。首次没有旧版本，就绪后直接开放服务，不执行双版本真实流量验证。

确认 `phase` 为 `COMPLETED`，再按 [README](../README.md) 配置路由、路由消费者和 Agent 凭据，验证实际代理与会话续接。`/readyz` 只提供入口就绪信号，不能证明业务调用成功；使用非默认监听地址时替换命令中的地址。首次部署如果还需要修改启动文件和监听配置，应在正式接入流量前完成并验证。

### 宿主更新

需要更新 host 与管理 CLI 时，先完成当前发布并暂停外部流量，排空请求后执行：

```sh
sudo aiagent-gateway stop
sudo systemctl status aiagent-gateway.service
sudo ./aiagent-gateway-linux-amd64 install --update-host
sudo aiagent-gateway rollout status --json
```

安装器确认服务已停止，备份配置、完整状态、CLI、unit 和 drop-in，保留业务状态及自定义 drop-in，归档 entry/release 进程身份，再用新产物启动 host 和初始 worker。备份位置和失败恢复边界见 [整套组件更新](DEPLOYMENT_LIFECYCLE.md#整套组件更新)。

## 日常灰度升级

适用于已经完成上述安装的机器。以下命令在新产物解压目录执行，不需要先停止或重启现有服务。

### 1. 保存发布前信息

```sh
sudo aiagent-gateway rollout status --json > rollout-before.json
sudo systemctl list-units --all 'aiagent-gateway*'
sha256sum ./aiagent-gateway-linux-amd64
```

记录报告中的旧 `stable.release`、`stable.artifact`、槽位和本次新产物哈希。`rollout-before.json` 用于后续对照和完成后的降级，不能代替运行状态备份；启动下一次发布后，控制器当前报告会更新。

如需调整比例、窗口和样本数，由 root 修改 `/etc/aiagent-gateway/deployment.json` 中的 `policy`，只影响下一次提交的发布；完整字段见下文。需要覆盖的每类请求必须有真实成功样本，`install` 不主动调用模型补样本。

### 2. 用新产物启动升级

```sh
sudo ./aiagent-gateway-linux-amd64 install
```

必须执行新上传二进制的 `install`。当前 `/usr/local/bin/aiagent-gateway` 保留为基础管理 CLI，普通升级不替换它；直接执行 `sudo aiagent-gateway install` 会提交该 CLI 自身的产物，可能不是期望发布的新版本。管理查询继续使用 `sudo aiagent-gateway rollout ...`。

安装器将新产物保存到 SHA-256 版本目录，提交后台任务并等待结果。旧实例继续服务，候选依次经过 5% 灰度、真实请求验证、100% 后续流量、全量观察和旧版排空。默认最短灰度 60 秒，再全量观察 300 秒，另外还需要启动和排空时间。

重复执行 `install` 按二进制内容的 SHA-256 判断版本，不依赖上传文件名：

- 若该版本已是当前稳定版本，安装器核对 worker 身份、就绪状态和入口指向后输出 `release already active`，返回成功；不新建发布任务，不启动另一 worker，也不重新等待流量验证。
- 若同一版本正在发布，继续等待原任务，保留原 rollout ID、验证进度和观察窗口。发布失败或回退仍返回非零，不把等待当成成功。
- 若有其他版本正在发布，或处于 `RECOVERY_REQUIRED`，保留现场并提示查询/恢复。稳定 worker 不健康或入口不一致时也不报告已安装成功。
- 服务组未启动时先按现有清单启动；缺失的基础 unit/CLI 仍会修复。重新安装当前版本不覆盖配置、凭据或业务数据。

已失败/回退的候选版本可再次执行 `install` 发起新发布；与当前稳定版本不同的产物仍须完成全部灰度验证。使用包含此逻辑的新上传二进制执行 `install`，无需先升级常驻控制器即可避免重复提交；旧的系统管理 CLI 不会由普通 worker 发布自动替换。

### 3. 查看进度

另开终端执行：

```sh
sudo aiagent-gateway rollout status --json
sudo journalctl -u aiagent-gateway.service -n 100 --no-pager
sudo journalctl -u aiagent-gateway.service -f
```

两槽位交替使用，不固定 B 为新版；以报告中的 `candidate.instance` 为准。日志用于定位问题，转正判定使用结构化代理结果。

`install` 终端断开不取消后台任务。重新连接后可查询状态，或再次用同一份新二进制执行 `install` 接续等待原任务。若 CLI 因无法读取控制器而返回非零，后台仍可能在运行，以恢复查询后的状态为准。

### 4. 验收并归档

```sh
sudo aiagent-gateway rollout status --json > rollout-after.json
sudo systemctl list-units --all 'aiagent-gateway*'
curl --fail --silent --show-error http://127.0.0.1:8080/readyz
```

确认 `phase == COMPLETED`，且 `stable.release` 与本次安装二进制的 SHA-256 一致。基础组件和新稳定 worker 应在运行，旧 worker 已停止属于正常情况；服务组的 `active (exited)` 也不代表各子服务都健康，需要结合子服务状态和实际请求确认。

使用升级前保留的会话或资源继续请求，核对仍访问原 Agent；再验证新会话、实际启用的流式/A2A 接口。归档发布前后报告、构建包和日志。保留旧版本产物以支持后续降级，不恢复旧路由存储或共享日志覆盖升级期间产生的数据。

## 结果确认和异常处理

`install` 只有发布完成返回 0；失败、已回退及需要恢复均返回非零值。`status` 展示阶段、版本、流量 revision、各类型结果、失败原因和恢复阶段。未结束发布中的 `stable` 是控制器保存的稳定目标，不能仅凭这个字段推断当前流量比例；结合阶段及日志判断。

| 状态 / 原因 | 操作 |
| --- | --- |
| `COMPLETED` | 核对新版本哈希、子服务和业务请求，归档结果 |
| `ROLLED_BACK` | 后续流量已恢复旧版，候选已收尾；查看失败原因，修复后重新执行新产物的 install |
| 样本不足 / `observation_evidence_incomplete` | 检查真实流量、必需类型和时间窗口；已回退的任务不能用 resume 强制通过 |
| `RECOVERY_REQUIRED` / `old_worker_drain_timeout` | 保留两版，等待旧请求和后台执行结束后 resume；此时后续流量仍走新版 |
| `RECOVERY_REQUIRED` / `candidate_drain_timeout` | 后续流量已回旧版，候选存量工作尚未结束；结束后 resume 完成回退 |
| 其他 `RECOVERY_REQUIRED` | 根据 `reason`、`resume_phase` 和组件日志恢复控制或存储故障，再 resume；不要删除发布日志绕过任务 |
| `FAILED` 或查询不到报告 | 检查启动、基础组件和控制器日志；尤其首次安装/迁移失败不能视为已自动恢复旧部署 |

排空超时不会强杀旧请求，也不会在请求稍后结束时自动恢复。处理完成后执行：

```sh
sudo aiagent-gateway rollout resume
sudo aiagent-gateway rollout status --json
```

`resume` 仅用于 `RECOVERY_REQUIRED`，重新进入记录的恢复阶段，不把验证失败改为成功。发布期间配置冻结返回 `409 ROLLOUT_IN_PROGRESS` 属预期行为，安全完成或回退后才解除。

### 发布期间主动回退

当活动任务仍保留候选和旧稳定目标时，可以主动请求回退：

```sh
sudo aiagent-gateway rollout rollback
sudo aiagent-gateway rollout status --json
```

命令等待后台切回后续流量并排空候选；已经发给 Agent 的请求不会重放，也不会撤销已执行的业务操作。

### 发布完成后降级

`COMPLETED` 后候选已清除，`rollout rollback` 不提供历史版本选择。需要降级时，使用此前保存的旧产物发起一次新的灰度发布；它同样需要兼容现有共享状态、获得真实成功样本并完成排空。

先从 `rollout-before.json` 查出目标旧版本哈希，确认该版本兼容当前状态，再替换下面变量并执行：

```sh
previous_release='填写发布前记录的64位SHA256'
sudo "/opt/aiagent-gateway/releases/${previous_release}/gateway" install
sudo aiagent-gateway rollout status --json
```

仅适用于已具备灰度能力且兼容当前共享状态的历史 worker 版本。不可通过覆盖运行中二进制、修改基础组件路径或恢复旧状态文件完成灰度降级。

## 默认发布规则

1. 检查旧版健康，冻结路由和 Endpoint 管理写入。
2. 在空闲槽位启动候选，校验内容哈希、进程身份、控制/状态协议及配置版本。
3. 将 5% 的新会话分配给候选；已有归属在灰度期间保持版本亲和。
4. 至少观察 60 秒，每个必需 profile 完成至少 10 次成功代理；验证最多 300 秒。
5. 通过后，后续请求全部切入新版，包括已有会话的后续请求；原 Agent Endpoint 不变，进行中的连接留在原进程。
6. 全量观察 300 秒，并取得新的成功样本。其间及旧版排空期间继续处理失败信号。
7. 等待旧 worker 的入口请求、共享执行租约和本地后台工作归零，再停止旧版、解除配置冻结。

候选有效失败阈值默认 0。连接错误、上游 5xx/429、网关错误、协议 error、SSE 非正常结束等会触发回退。成功样本不足、上报证据缺失或未知最终结果均不能转正。普通客户端错误和有证据的主动取消不计成功；健康探测、Agent Card 等本地响应也不计真实代理成功。

成功样本按完成的请求计数，正在进行的候选请求不算成功样本，但不会阻塞已有足量样本的阶段推进；后续失败仍在当前发布保护期内触发回退。保护期结束后运行故障由常规重启/告警处理，不再自动回退上一版。

回退恢复的是后续流量。已经发送给 Agent 的请求不自动重放，不能承诺挽救当前失败请求或撤销已执行的工具操作。候选存量连接和后台执行安全退出后，才报告 `ROLLED_BACK`。旧版不可用、控制状态无法确认或排空超过 600 秒时保留现场并报告 `RECOVERY_REQUIRED`。

## 发布策略

root 编辑 `/etc/aiagent-gateway/deployment.json` 的 `policy` 对象，保留同一文件内的基础组件和初始版本路径。下次 install 读取新策略，不改变正在运行的发布。

```json
{
  "auto_profiles": true,
  "canary_percent": 5,
  "readiness_timeout_seconds": 30,
  "verify_min_seconds": 60,
  "verify_timeout_seconds": 300,
  "min_success_per_profile": 10,
  "max_effective_failures": 0,
  "required_profiles": ["http"],
  "observe_seconds": 300,
  "observe_min_success_per_profile": 10,
  "drain_timeout_seconds": 600
}
```

`auto_profiles: true` 按启用路由推导 HTTP、Hermes SSE、A2A 和 A2A SSE 必需项；验证期间观察到其他已成功代理的受支持 profile 时，也加入后续门槛。设为 false 才完全使用 `required_profiles`。`http`、`sse`、`a2a`、`a2a_sse` 是四个独立 profile，不能相互抵充样本。

generic SSE 需要可识别的结束信号，例如 `[DONE]`；没有结束定义的无限流不能作为完整成功样本。当前没有主动业务探测和流量镜像功能。低流量环境需要提供足够的正常请求或适当调整观察时长/样本数；安装器不会自动调用模型补足样本。

灰度比例针对新分配的逻辑会话，少量长会话的请求量比例不一定等于 5%。所有请求经过同一个入口后，已有 HTTP keep-alive 连接上的新请求也使用更新后的策略。

## 服务、端口与数据

| 角色 | systemd 管理 | 监听 |
| --- | --- | --- |
| host：入口、管理、状态、发布控制 | aiagent-gateway.service，非 root MainPID | 数据 :8080、管理 :9090；内部控制 UDS |
| worker A/B | host 的一至两个直接子进程，同一 cgroup | 动态分配的 loopback 数据/内部管理端口 |

公共管理 API 由 host 提供，请求携带管理 Token。发布/入口控制 UDS 还校验本机 root 安装身份；普通业务管理 Token 不能通过远程公共 API 启动二进制。

```sh
sudo journalctl -u aiagent-gateway.service -f
sudo systemctl show aiagent-gateway.service -p MainPID -p ControlGroup
sudo aiagent-gateway status
```

`stop/start/restart` 是整套服务维护操作，可能中断连接；业务升级使用 install。host 崩溃后子进程退出，新 host 校验日志，未提交的灰度回退旧稳定版，已进入提交阶段的恢复新稳定版。业务升级期间 host PID 和状态 epoch 不变。

关键路径：

- `/opt/aiagent-gateway/releases/<sha256>/gateway`：root 暂存的只读业务产物。
- `/opt/aiagent-gateway/infrastructure/<sha256>/gateway`：固定 host 产物。
- `/var/lib/aiagent-gateway/config.json`、`routes.json`、`route-logs.json` 和恢复事务：配置与管理记录。
- 同目录 `shared/state.log`：资源、别名、幂等和会话；`entry/state.log`：切流、亲和与结果；`release/state.log`：发布报告与切流意图。

所有 worker 共用 host 的状态、额度和列表快照。状态 epoch 变化后旧客户端停止写入，资源定位和幂等元数据可从持久日志恢复；TCP 连接、短期分页游标和上游执行本身不能靠这些日志恢复。未知结果保留归属，不重新创建有副作用的操作。

卸载移除服务和 CLI，保留凭据、状态、发布报告、清单和产物，供重装使用。host 自身升级与不兼容状态变更要安排维护流程，不能用旧状态覆盖灰度中新建资源。完整文件布局和恢复步骤见 [部署全流程](DEPLOYMENT_LIFECYCLE.md)。

## 本机验证

```sh
go test ./...
go test -race ./...
go vet ./...
go build -o /tmp/aiagent-rollout-check ./cmd/gateway
python3 deploy/rollout-local/check.py /tmp/aiagent-rollout-check
```

`check.py` 在临时目录构建两个不同版本，验证 host 的 2 → 3 → 2、连续 A → B → C、失败回退、长 SSE 排空超时/resume、宿主 PID/epoch、稳定 worker 崩溃、父进程死亡、重启恢复、原 Agent 绑定及无孤儿。脚本只使用本机临时端口和模拟上游，退出时清理。Go 测试另外覆盖子进程身份/租约/回收、控制 UDS 身份、重复安装幂等、宿主更新备份及发布日志冲突恢复。

这些检查不修改本机既有 Hermes 部署，也不替代 Linux systemd 实机、真实模型或生产压力验证。
