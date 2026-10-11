# 自动灰度升级操作说明

状态：代码已实现，已通过本机真实多进程 HTTP 切流和自动发布状态机测试；Linux systemd 实机部署、真实模型升级联调及生产压力验收尚未完成。以下命令是当前实现的接口。

组件改造、会话到原 Agent 的映射持久化、代码位置及测试范围见 [实现过程记录](ROLLOUT_IMPLEMENTATION_RECORD.md)。

操作顺序：[构建发版](#构建发版) → [上传与安装前检查](#上传与安装前检查) → [首次安装或维护迁移](#首次安装或维护迁移) / [日常灰度升级](#日常灰度升级) → [结果确认和异常处理](#结果确认和异常处理)。

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
- 首次安装时外部端口及内部 A/B 端口可用；已有部署升级时，稳定槽位已占用自己的端口属正常情况，候选槽位端口不能被其他程序占用。端口表见下文，可用 `sudo ss -ltnp` 核对监听进程。
- 已有灰度部署的 `rollout status --json` 可读取，当前没有未结束发布；处于 `RECOVERY_REQUIRED` 时先处理该任务。
- 新版兼容当前基础组件的控制/状态协议，且灰度中新建资源仍可被旧版读取。需要升级基础组件或变更不兼容状态格式时，安排独立维护迁移。
- 灰度窗口内有足够的新会话和相应 HTTP/SSE/A2A 请求。只有存量会话或没有流量，可能无法取得候选成功样本；先确定发布策略，不在验证中修改配置。

## 首次安装或维护迁移

新版只读取 gateway.yaml，YAML 不再配置 callers；消费者随路由 POST 的 consumers 动态写入。旧环境变量版本的基础组件和 worker 必须一起维护迁移，普通 install 会拒绝旧部署清单；先按 [YAML 配置迁移](CONFIGURATION.md#从环境变量版本迁移) 准备文件和基线。

### 全新服务器

在已解压并校验的目录执行：

```sh
chmod +x ./aiagent-gateway-linux-amd64
sudo ./aiagent-gateway-linux-amd64 install
sudo aiagent-gateway rollout status --json
sudo systemctl list-units --all 'aiagent-gateway*'
curl --fail --silent --show-error http://127.0.0.1:8080/readyz
```

安装器创建系统用户、凭据、配置及服务，启动基础组件和初始 worker，并启用服务组开机启动。凭据保存在 `/etc/aiagent-gateway/gateway.yaml`，不输出到终端。首次没有旧版本，就绪后直接开放服务，不执行双版本真实流量验证。

确认 `phase` 为 `COMPLETED`，再按 [README](../README.md) 配置路由、路由消费者和 Agent 凭据，验证实际代理与会话续接。`/readyz` 只提供入口就绪信号，不能证明业务调用成功；使用非默认监听地址时替换命令中的地址。首次部署如果还需要修改环境文件和监听配置，应在正式接入流量前完成并验证。

### 旧单进程部署首次迁移

没有灰度部署文件、仍使用原单进程服务的机器，需要维护窗口：

1. 暂停新业务请求，等待原请求及网关本地后台工作结束；另行安排仍需旧内存映射访问的 Agent 任务。
2. 保存原二进制、旧 `gateway.env`、新 `gateway.yaml`、`config.json` 和原 systemd unit 的备份，保留其文件权限。备份配置不能恢复进程内会话。
3. 在新产物目录执行迁移命令。
4. 确认发布完成和各组件状态，使用新的会话验证代理后恢复业务流量。

```sh
sudo ./aiagent-gateway-linux-amd64 install --migrate
sudo aiagent-gateway rollout status --json
sudo systemctl list-units --all 'aiagent-gateway*'
```

迁移保留凭据、路由及 Endpoint ID，但当前旧版本的内存 session/task 映射不能导出，会按原有重启边界失效。不能把首次迁移当成无中断升级。迁移启动失败时需根据服务日志恢复，当前没有完整的旧部署自动快照回滚；不要手动覆盖已经产生新资源的共享状态。

## 日常灰度升级

适用于已经完成上述安装或迁移的机器。以下命令在新产物解压目录执行，不需要先停止或重启现有服务。

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

重复部署同一内容仍会执行验证流程；没有实际流量时也不会跳过验证。

### 3. 查看进度

另开终端执行：

```sh
sudo aiagent-gateway rollout status --json
sudo journalctl -u aiagent-gateway-rollout.service -n 100 --no-pager
sudo journalctl -u aiagent-gateway-worker-a.service -u aiagent-gateway-worker-b.service -f
```

两槽位交替使用，不固定 B 为新版；以报告中的 `candidate.instance` 为准。日志用于定位问题，转正判定使用结构化代理结果。

`install` 终端断开不取消后台任务。重新连接后先查询状态，不重复提交。若 CLI 因无法读取控制器而返回非零，后台仍可能在运行，以恢复查询后的状态为准。

### 4. 验收并归档

```sh
sudo aiagent-gateway rollout status --json > rollout-after.json
sudo systemctl list-units --all 'aiagent-gateway*'
curl --fail --silent --show-error http://127.0.0.1:8080/readyz
```

确认 `phase == COMPLETED`，且 `stable.release` 与本次安装二进制的 SHA-256 一致。基础组件和新稳定 worker 应在运行，旧 worker 已停止属于正常情况；服务组的 `active (exited)` 也不代表各子服务都健康，需要结合子服务状态和实际请求确认。

使用升级前保留的会话或资源继续请求，核对仍访问原 Agent；再验证新会话、实际启用的流式/A2A 接口。归档发布前后报告、构建包和日志。保留旧版本产物以支持后续降级，不恢复旧 `config.json` 或共享日志覆盖升级期间产生的数据。

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

仅适用于已具备灰度能力且兼容当前共享状态的历史 worker 版本；回到首次迁移前的单进程版本属于维护恢复。不可通过覆盖运行中二进制、修改基础组件路径或恢复旧状态文件完成灰度降级。

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

| 角色 | systemd unit | 默认端口 |
| --- | --- | --- |
| 服务组 | aiagent-gateway.service | 无 |
| 固定入口 | aiagent-gateway-entry.service | :8080 |
| 配置及共享状态 | aiagent-gateway-state.service | 管理 127.0.0.1:9090；状态 Unix Socket |
| 发布控制器 | aiagent-gateway-rollout.service | 受保护的 Unix Socket |
| A/B 业务实例 | aiagent-gateway-worker-a.service / worker-b.service | 18080/18081；内部管理 19090/19091，均为 loopback |

公共地址通过 YAML 的 `listen`、`admin_listen`、`public_url` 配置。内置 systemd driver 的 A/B 端口目前固定，部署前须确保空闲，端口冲突会拒绝候选启动。前台 worker 模式允许使用 `--listen`、`--admin` 指定其他内部端口。

```sh
sudo journalctl -u aiagent-gateway-rollout -f
sudo journalctl -u aiagent-gateway-worker-b -f
sudo aiagent-gateway status
sudo aiagent-gateway stop
sudo aiagent-gateway start
```

`stop/start/restart` 是整套服务的运维操作，可能中断连接；升级使用 install。控制器会在服务组恢复后启动已确认的稳定版本并更新进程身份，正在进行的发布若遇到 worker 重启则进入回退/恢复流程。

关键路径：

- `/opt/aiagent-gateway/releases/<sha256>/gateway`：不可变业务产物。
- `/opt/aiagent-gateway/infrastructure/<sha256>/gateway`：固定入口、控制器和状态服务使用的基础产物。
- `/var/lib/aiagent-gateway/config.json`：原管理配置，仍由单个状态服务写入。
- `/var/lib/aiagent-gateway/shared/state.log`：资源、别名、幂等和会话持久日志，内置压缩快照。
- `/var/lib/aiagent-gateway/entry/state.log`：入口路由、灰度亲和和有界结果证据。
- `/var/lib/aiagent-gateway-rollout/state.log`：root 管理的发布状态和未完成切流意图。

worker 不再打开配置目录写锁，资源创建和公开 ID 提交由状态服务串行确认。两版共享并发额度及列表快照；短期列表游标支持切换 worker，但目前不跨状态服务重启恢复。状态服务改变运行代次时，旧 worker 会被拒绝继续写入并退出，避免迟到写入覆盖新状态。

共享状态保存的是资源定位和幂等元数据，不恢复 TCP 连接或上游工具执行。结果未知的创建操作保持归属，不因 worker 失联直接重新创建。资源空闲时间的持久化按分钟合并，恢复时可能多保留不到一分钟。

基础组件升级、状态协议不兼容迁移仍需独立维护流程；普通 install 不自动替换它们。当前资源日志提交和入口证据均有同步落盘成本，需要在目标机器测量容量后再确定生产并发参数。

卸载停止服务并移除 unit 和 CLI，保留凭据、共享状态、发布记录和版本产物以便重新安装。安装过程不得通过旧配置快照覆盖灰度中新建的合法资源。

## 本机验证

```sh
go test ./...
go test -race ./...
go vet ./...
go build -o /tmp/aiagent-rollout-check ./cmd/gateway
python3 deploy/rollout-local/check.py /tmp/aiagent-rollout-check
```

Python 检查只启动临时端口、临时数据目录下的 state、entry 和两个独立 worker，以及模拟 HTTP 上游，退出时清理。覆盖实际 5% 分配、全量切换、原会话归属、失败 POST 不重放和回退。自动发布状态机的通过、失败、观察期故障、排空和丢失切流回执恢复由 Go 测试验证。

这些检查不修改本机既有 Hermes 部署，也不替代 Linux systemd 实机、真实模型或生产压力验证。
