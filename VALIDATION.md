# 验证记录

## 管理与业务共用 HTTP 端口（2026-10-27）

移除 admin_port 配置与独立管理 listener；默认通过同一 8080 端口访问 Agent、/admin/v1/... 和 /metrics。管理接口与指标保留管理 Token 鉴权，调用方 Token 无管理权限。前台、完整 process、正式 systemd 服务及候选进程统一使用同一套分发；管理请求单独设置 I/O 期限并在返回时清除，SSE 没有继承管理响应期限。发布控制仍通过 UDS 验证本机 UID 与凭据。

- 全量 go test -race -count=1 ./...、go vet ./... 通过。新增真实 HTTP 同端口鉴权及私有路径隔离检查；实际候选子进程在交接前将管理与指标转发到源 owner，交接后改由新 owner 处理，前后认证和指标内容均通过。
- 普通前台和 process 两种入口分别通过单 PID、同端口创建路由、管理员/调用方权限隔离、上游精确一次执行、活动 SSE 停机排空及重启恢复。Linux amd64 静态构建通过。
- [Debian/systemd 完整流程](docs/validation/2026-10-27-single-port-systemd.json)通过正式安装、A→B→C、CLI 断开继续、失败回退、SIGKILL 恢复、双进程 stop/start、保留数据卸载重装、容器重启与启动清理。678 个短时模拟请求无意外失败或队列超时/拒绝，服务进程最多两个。没有额外长期压测。
- [真实 Hermes 联合验收](docs/validation/2026-10-27-single-port-real-agent.json)整轮 passed=true，含同端口管理/指标与鉴权、HTTP/SSE/A2A/A2A SSE、记忆与调用方隔离、缺样本回退后再发布、MainPID 接管和重启后旧会话/任务恢复及启动清理。通过 /proc 将服务进程 socket inode 与 TCP LISTEN 表匹配，启动、换版后和重启后实际监听端口均为 [8080]。MainPID 178 → 332，正常发布 systemd 重启数 0；重启后 MainPID 73。最终四类直接入口成功数 HTTP/SSE/A2A/A2A SSE 为 3/1/5/2，失败 0、活动请求 0、证据无丢失；交接窗口 294 ms，整轮约 215.25 秒。
- [真实 Agent 首轮脚本失败](docs/validation/2026-10-27-single-port-real-agent-counts-failed.json)保留：已通过发布前四 profile，但 VERIFYING 初始报告的 counts 为 null，脚本未处理空统计而抛出 AttributeError。将空统计视为尚无样本后独立重跑通过，没有改变样本门槛或模型断言。

继承布局由两个公共 TCP socket 加控制 UDS 缩为一个公共 TCP socket 加控制 UDS，候选协议升级为 3。旧双端口候选与该布局不兼容，会在启动/握手阶段被拒绝；没有实施旧部署自动迁移。旧 YAML 需删除 admin_port 字段。历史双端口报告按原版本保留；物理断电与长期容量仍按用户要求暂缓。

## Linux/systemd 与真实 Agent 联合验收完成（2026-10-27）

[完整通过报告](docs/validation/2026-10-27-systemd-real-agent-passed.json)已取得 passed=true。使用 Debian 12 / systemd 252.39 / Linux arm64 容器内的正式安装器和两个隔离 Hermes 实例，沿用原模型配置与安全策略；本轮没有修改 Go 运行时代码。

- [直连上游诊断](docs/validation/2026-10-27-real-agent-startup-diagnostic.json)复现启动竞态：Hermes HTTP/A2A 已监听、状态已标记 running，但启动恢复的入站门禁尚未打开。四个请求被入队，同时返回空的 COMPLETED；日志确认模型在门禁释放后才执行。验收现在等待固定镜像在完成启动恢复后写出的结束标记，再发业务请求；不会用重试任务掩盖空回复，也不导出完整 Agent 日志。
- 仅补就绪等待后的[独立运行](docs/validation/2026-10-27-systemd-real-agent-startup-gate-recall-failed.json)仍在发布前未通过标签回忆检查。另一次直连观察到模型明确拒绝执行 A2A 中的嵌入指令；将原先“记住标签、仅回复 SAVED”改为讨论带随机名称的虚构水果展览，第二轮询问展览名称。报告标记 fictional_exhibition_v1；仍要求回复包含首轮的精确随机名称，保留非空回复、完整流、task/context 身份、分页与调用方隔离断言。没有关闭 A2A 安全包装或替换模型。
- 发布前四类真实请求、会话记忆和隔离通过；故意不给候选样本时自动 ROLLED_BACK，未提交、旧进程保留。随后重新发布，在观察期补齐直接入口样本并 resume 一次，最终 COMPLETED 且 committed=true。
- MainPID 从 179 接管为 332，始终使用非 root UID 986，正常发布的 systemd 重启数为 0，完成后只剩一个服务进程。最终 HTTP/SSE/A2A/A2A SSE 直接入口成功数为 6/2/3/1，另有 3 个 excluded，失败和活动请求为 0，证据无丢失。快照 491978 字节，交接窗口 317 ms；整轮 194.893 秒，wall/monotonic 无差异。
- 交接后和网关 Linux 容器重启后，原 HTTP 历史、A2A task/context ID 与调用方隔离均通过。重启选择已提交版本，MainPID 为 75；运行中清理被拒绝，冷启动删除两个过期额外产物和临时快照、保留两个近期额外产物。两个 Hermes 实例在网关重启期间继续运行。

本轮修改仅涉及验收脚本、报告和文档；脚本语法与报告完整性另行检查。原失败报告保留，不将先前失败改为通过。按用户最新要求，物理断电暂缓；长期稳定性与容量边界验证继续跳过，未测项不标为通过。此前 ext4/XFS、Debian/Ubuntu 生命周期和 Go 检查仍按各自报告范围有效。

## 冷启动清理、文件系统恢复与快照复核（2026-10-27）

先按用户要求提交已有改动为 63f243c，再继续本轮工作。长期稳定性与容量边界验收按本轮用户指示跳过；以下系统生命周期、存储故障测试和定向内存基准不作为长期容量证明。

- 增加冷启动前自动清理与显式 cleanup 命令。安装锁及 owner 锁共同排除正在安装、运行或交接的网关；只有持久报告已结束且没有待恢复交接才回收。保留初始启动产物、当前决定和发布报告的全部引用、unit 维护产物、当前快照及两个最近的额外产物。未知/损坏目录、额外文件和符号链接保留，业务日志和发布决定不改写。原样生成的旧 unit 可补齐维护入口，定制 unit 不覆盖。
- 新增回归覆盖运行中拒绝清理、未决 handoff/rollout、损坏决定、缺失启动产物、损坏快照、重复清理、冷启动仍选当前版本、保留维护产物，以及符号链接/未知文件隔离。
- [Debian/systemd 完整验证](docs/validation/2026-10-27-systemd-cleanup.json)通过正式安装、A→B→C、失败回退、CLI 断开、SIGKILL 恢复、双进程 stop/start、卸载保留数据后重装和容器重启。运行中 cleanup 被拒绝；重启前建立的过期产物和临时快照被自动回收，近期额外产物保留，已提交版本及业务配置恢复。675 次串行模拟请求无意外失败或队列超时/拒绝，服务进程峰值 2；没有执行额外长时发布轮次。早期[清理测试失败](docs/validation/2026-10-27-systemd-cleanup-fixture-failed.json)源于测试把四个样本设为很旧的时间，却未计入已有近期产物；修正测试的时间排序后通过，未放宽保留策略。
- [ext4/XFS 矩阵](docs/validation/2026-10-27-storage-matrix.json)在 Linux 6.10.14-linuxkit / arm64 中分别创建并挂载私有 512 MiB 镜像，以 UID 65534 完整运行 state、handoff、entry、process、service 五包测试，含现有完整网关强杀和存储错误注入回归；两种文件系统均全部通过。卸载后的 e2fsck -f -n / xfs_repair -n 均返回 0。运行阶段无网络、无宿主目录挂载，没有操作宿主磁盘。首轮[测试目录权限失败](docs/validation/2026-10-27-storage-harness-permissions-failed.json)在执行测试前退出，修复目录可遍历权限后复测。整个矩阵约 41 秒，不是物理断电实验。
- 提交时已有校验过的镜像，现以 32 KiB 缓冲从磁盘逐块比较，并独立核对完整 SHA-256；省去再次读取为完整内存副本。仍校验 owner/决定/预算/文件类型/长度，拒绝被修改的内存副本、磁盘内容、两者同时损坏及符号链接。没有取消独立最终快照。
- [8 MiB 快照定向基准](docs/validation/2026-10-27-maintenance-snapshot-memory.json)，每路径 10 次：原 LoadSnapshot 后比较为 41,705,161 B/op、49 次分配；逐块比较为 34,184 B/op、13 次分配。测得约 7.22 / 3.77 ms，仅描述该文件复核操作，分配总量不是进程峰值 RSS，也不是生产交接时延或吞吐结论。
- 最终版本的[全量竞态](docs/validation/2026-10-27-maintenance-final-go-race.json)、[vet](docs/validation/2026-10-27-maintenance-final-vet.json)及 [Linux amd64 构建](docs/validation/2026-10-27-maintenance-linux-amd64-build.json)通过。竞态使用 count=1；Python 脚本语法另行检查。

Linux/systemd 与真实 Hermes 的联合脚本已接入正式安装、非 root/凭据、MainPID 接管、零 systemd 重启、旧会话/任务恢复和启动清理。一次[运行](docs/validation/2026-10-27-systemd-real-agent-reboot-port-failed.json)通过四 profile、缺样本回退后成功发布、交接后资源检查及重启选版，但在重启后的宿主请求因 Docker 随机端口变化失败；脚本已固定 loopback 端口。其后模型请求分别出现[需要输入](docs/validation/2026-10-27-systemd-real-agent-stream-failed.json)、[空任务](docs/validation/2026-10-27-systemd-real-agent-cleanup-empty-task.json)和[标签回忆失败](docs/validation/2026-10-27-systemd-real-agent-recall-failed.json)，该阶段最后一次失败发生在发布前，公开 task/context ID 保持且流到达 COMPLETED，但回复未包含首轮标签。该阶段尚未取得整轮通过；后续定位和最终联合通过记录见本文首节。此前 macOS 网关配合真实 Hermes 的通过报告仍按原环境保留。

[Ubuntu 24.04 / systemd 255.4 验证](docs/validation/2026-10-27-systemd-noble.json)使用独立 Noble 镜像，通过相同的正式安装、A→B→C、失败回退、CLI 断开、SIGKILL、双进程停启、卸载重装和容器重启流程；启动清理的运行中拒绝、过期产物回收、近期版本保留和临时文件删除均通过。671 次串行模拟请求无意外失败或队列超时/拒绝，服务进程峰值 2。Debian 与 Ubuntu 是不同用户空间/systemd 版本，仍共享 Docker Linux VM 内核；这里未声明所有 Linux 发行版或物理设备通过。

上述存储测试不能证明物理掉电和具体生产存储设备已经验收；断电现按用户要求暂缓。完整真实模型联合验收后续已通过，见本文首节。旧实验部署自动迁移在既定方案中明确不做，不作为本轮遗漏功能。

## 真实 Agent 完整换版与最终复核（2026-10-26，10 月 27 日归档）

续接会话后核对已结束的后台任务，归档此前尚在 `/tmp` 的[真实 Hermes 成功报告](docs/validation/2026-10-26-receipts-real-agent-passed.json)，原始内容未改写。该轮在 10 月 26 日 17:06–17:11（Asia/Shanghai）执行，使用 macOS arm64 网关及两个独立数据目录、凭据的 Linux Hermes 容器；Agent 镜像摘要、两份网关产物 SHA-256 和工作区修改标记均在报告中。它不等于 Linux/systemd 与真实模型组合的验收；Linux 运行与系统接管证据分别见下文。

- 发布前通过 HTTP 会话/记忆续接、原生 SSE、A2A 同步/流式任务、context、任务查询、分页和调用方隔离。
- 故意不给候选业务样本，确认以 `insufficient_successful_proxy_samples` 自动回退、未提交并保留旧进程；随后再次发布相同候选。
- 四类真实请求通过候选验证。观察期缺直接入口样本时进入 `RECOVERY_REQUIRED`，补齐 HTTP/SSE/A2A/A2A SSE 后显式 resume，最终达到 `COMPLETED`、确认候选成为已提交稳定产物且源进程正常退出。
- 完成时四类直接入口成功数分别为 HTTP 3、SSE 1、A2A 5、A2A SSE 2，另有 6 个 excluded 结果；失败 0、活动请求 0、证据无丢失。快照 494765 字节，完整交接窗口 284 ms。总运行 270.910 秒，wall/monotonic 差 0.001 秒。
- 交接后仍能读取原 HTTP 会话历史和两个原 A2A 任务，公开 task/context ID 保持，另一调用方查询为空，两个 Agent 实例的 HTTP 在途占用均为 0。验收使用缩短的 2 秒观察期和少量合成模型请求，不作为模型服务长期稳定性或生产容量结论。

原报告的 `last_rollout` 是补样本前的暂停诊断快照；最终统计在 `checks.rollout`，交接后断言在 `checks.after`，只有所有断言通过才写 `passed=true`。归档保留这一原始字段；脚本现已在完成时刷新 `last_rollout`，避免后续报告保留过时阶段。

此前两轮失败独立保留：[暂停统计轮](docs/validation/2026-10-26-receipts-real-agent-paused-statistics.json)因脚本重复读取暂停时的旧统计而未 resume；[记忆检查轮](docs/validation/2026-10-26-receipts-real-agent-recall-failed.json)在发布前 HTTP 第二轮未回忆出首轮标识，但诊断确认会话历史仍有首轮内容。修复暂停流程后进行独立复核，记忆、流完成及所有发布断言均未放宽，也不将记忆检查失败归因于换版。

最终输入校验修正后的[全量竞态测试](docs/validation/2026-10-26-receipts-final-go-race.json) `go test -race -count=1 ./...` 和 [vet](docs/validation/2026-10-26-receipts-final-vet.json) 均已通过，耗时 76.111 / 0.620 秒；这些结果来自上一会话已结束的运行，本次归档未重复执行 Go 全量测试。

[最终新会话增长复测](docs/validation/2026-10-26-receipts-concurrency-100-final.json)以非 root Linux arm64 / overlayfs 运行，HTTP 和 SSE 分阶段各保持 100 并发，每个请求使用新会话，完成 A→B→C。HTTP 11371 次、SSE 4116 次，总计 15487 次请求全部成功，无重复上游执行、队列超时或拒绝；HTTP/SSE p95 为 158.109 / 485.755 ms，最长候选分配暂停 1031.200 ms。服务进程峰值 2，合计峰值 RSS 536704 KiB、FD 379；两轮结束单进程 FD 均为 23。总运行 26.412 秒，wall/monotonic 差为 0；测量期间未同时运行 Go 测试。这补充了下文复用会话池的 10 轮长流回归，仍属于开发宿主上的短时测量。

## 紧凑发布回执与混合长流（2026-10-26）

续接长流验收中发现的入口证据容量问题。旧实现单次发布最多保留 8192 个完整结果；原失败轮虽然 14404 个业务请求全部成功，控制器仍因 `result_evidence_lost` 回退，见[原失败报告](docs/validation/2026-10-26-receipts-legacy-capacity-failed.json)。

- 新旧进程通过 `compact_results` 显式协商。双方支持时保存开始时间、执行 incarnation、profile、verdict、是否直接入口及完整结果的 SHA-256，单次发布最多 65536 条；仍按原时间窗口和全部失败统计，不抽样或丢弃失败。达到新上限继续标记证据丢失，禁止凭不完整证据通过发布。
- 完整结果摘要包含 release、instance、入口身份、路由 revision、状态码、原因和完成时间；时区和单调时钟成分规范化后比较，完全相同的重试可确认，任一内容变化均拒绝。重启时保留已完成回执，在途请求记为失败；换版导入本身不写盘或把在途请求误判成重启失败。
- 完整镜像、记录索引和操作增量均携带回执格式与内容；导出/导入复制 Map，拒绝混合格式、损坏摘要及活动/完成 ID 重叠。仅在发布外切换格式，发布结束清空证据后可交接回旧版；旧源到新候选继续使用完整结果。
- 协商降级只接受通过帧、动作和序号校验的明确“不支持”错误；关闭连接、超时、损坏帧、错误动作/序号及错误身份不会被当成兼容确认。旧版错误回复不携带身份，沿用已完成身份握手的继承 socketpair。
- [全量测试](docs/validation/2026-10-26-receipts-go-tests.json)、[全量竞态](docs/validation/2026-10-26-receipts-go-race.json)与 [vet](docs/validation/2026-10-26-receipts-vet.json)通过。新增专项验证 8200 条真实分配/完成及重启、65536 条边界、统计窗口和直接入口计数、精确重试冲突、无别名交接、增量一致性及旧格式重置。
- [全新版竞态换版](docs/validation/2026-10-26-receipts-complete-race.json)和[新旧版竞态换版](docs/validation/2026-10-26-receipts-mixed-race.json)通过 A→B→C、失败回退、排空超时/resume、重新发布回 A 和冷启动选版。混合版本 A 来自 `ce05e32`，不支持紧凑发布回执；B/C 来自本轮工作区。耗时分别为 56.730 / 49.241 秒，没有 wall/monotonic 时钟跳变。
- [Linux 运行报告](docs/validation/2026-10-26-receipts-linux-tests.json)覆盖 entry/process 全包，非 root UID 65534、Linux arm64、网络隔离；entry 测试日志位于 overlayfs，process 临时进程路径位于可执行 tmpfs。Linux amd64 静态构建通过。

[10 轮混合长流报告](docs/validation/2026-10-26-receipts-mixed-long-streams.json)使用 100 个闭环客户端，其中 50 个 HTTP（模拟上游等待 50 ms）、50 个 SSE（首事件后持续 3 秒）；同一状态目录连续切换 A/B/C，共完成 10 次换版。每客户端复用 4 个会话，并在 VERIFYING 使用每轮独立会话池获取候选样本。观察期为 8 秒，状态等待仍为 2 秒、交接屏障仍为 5 秒。

| 指标 | 结果 |
| --- | --- |
| HTTP / SSE 请求 | 166565 / 3800 |
| 总请求 / 失败 / 重复上游执行 | 170365 / 0 / 0 |
| 发布完成 / 服务进程峰值 | 10 / 2 |
| HTTP p95（各轮范围） | 71.702–88.296 ms |
| 完整 SSE p95（各轮范围，含 3 秒流时长） | 3137.623–3333.045 ms |
| 最长候选分配暂停 | 981.484 ms |
| 完整快照 | 12717280–13884254 字节 |
| 两进程合计峰值 RSS / FD | 576084 KiB / 360 |
| 峰值 CPU（单核记 100%） | 247.606% |
| 每轮结束单进程 FD | 均为 23 |
| 总运行时间 / wall 与 monotonic 差 | 233.120 秒 / 0.001 秒 |

运行环境为非 root Linux arm64 / overlayfs、8 个可见 CPU、同容器模拟上游。测量期间没有运行 Go 测试；同一宿主曾同时运行低并发真实 Agent 验收。这是约四分钟、会话池有限的连续换版回归，不能据此宣布生产容量、小时级稳定性或内存无泄漏；发布后的 RSS 为 70964–116028 KiB，未做长期趋势推断。

## 连续跨进程操作增量（2026-10-25 至 2026-10-26）

本轮继续完成状态交接的连续增量，沿用原 2 秒状态等待、5 秒交接屏障和 32 MiB 增量预算。模拟上游测试不调用实际模型，不改动既有部署。

- 首次快照建立稳定记录索引，状态变更在领域临界区内写入有界操作链。后续 Follow 原子应用更新/删除，不重复捕获全量检查点；最终封存仍独立捕获完整快照，核对候选重建结果的精确摘要与业务不变量。
- 操作链校验 epoch、handoff ID、连续 logical_revision、SHA-256、精确确认前缀及批次/镜像预算。错误基线、缺口、损坏摘要、未知字段、尾随内容、更新/删除冲突均拒绝，失败批次不改变镜像。确认期间发生的新写入仍能继续追赶。
- 新增对照测试覆盖会话、资源/幂等预留、租约、分页缓存、回执淘汰、配置变更、维护和调度缓存；累计超过 8192 条回执后，增量重建与真实镜像逐字节一致。入口分配、结果完成、死亡入口清理、流量切换和证据重置也与实时状态逐项一致。
- 复制日志仅在业务日志同步后接收持久状态变更；业务 fsync 失败不进入复制链，日志压缩不重置复制序号。复制溢出只终止交接准备，已提交及后续业务写入仍可持久化。
- 修复准备中止时在途操作导致逻辑序号回退，以及溢出后序号停滞；无关 handoff ID 的中止请求不能关闭当前复制。源关闭后释放临时记录。没有启用复制时不额外复制共享回执。
- 实际候选子进程验证活动 SSE 跨增量接管完成、不重放上游请求；预览 ACK 不能授权提交。即使操作链摘要有效，只要重建状态与独立最终镜像不同，也拒绝接管、恢复旧 owner，并允许新基线重新发布。
- 私有协议和快照格式不变。`operation_stream` 通过显式命令协商；只有旧候选明确返回不支持才采用检查点差量，传输错误、超时和错误确认不能触发降级。旧源未协商扩展时，新候选不返回新增字段。
- [混合版本普通构建](docs/validation/2026-10-26-operations-mixed-versions.json)以 `6c8b7c3` 构建 A，以当前代码构建 B/C，完整通过 A→B→C、失败候选回退、排空超时/resume 后重新发布到 A、缺样本回退和冷启动选版。这里验收的是同一快照格式下的双向兼容。
- [混合版本竞态失败报告](docs/validation/2026-10-25-operations-mixed-race-failed.json)和[全新版受休眠干扰的失败报告](docs/validation/2026-10-26-operations-race-sleep-interrupted.json)单独保留，均曾在排空恢复后的交接阶段超时。全新版该轮运行计时仅 54.943 秒，报告中的 VERIFYING→PROMOTING 却跨越约 24 分钟；宿主机电源日志确认 02:54:07 入睡、03:18:34 唤醒及随后的反复休眠。因此该轮不能用来推断正常运行下的屏障时延。后续复核仅对测试进程临时防止空闲休眠，并记录 wall/monotonic 两种耗时，没有修改网关预算。
- 10 月 26 日复核：[全新版竞态发布](docs/validation/2026-10-26-operations-complete-race.json)和[混合版本竞态发布](docs/validation/2026-10-26-operations-mixed-race.json)全部通过，包含此前失败的排空超时/resume 后交接回 A、失败回退及冷启动。两轮分别耗时 53.595 / 50.169 秒，wall 与 monotonic 计时差小于 1 ms；未修改任何网关等待预算。此前失败报告继续保留，不以新结果覆盖旧记录。
- 本机重新执行的[全量测试](docs/validation/2026-10-26-operations-go-tests.json) `go test -count=1 ./...`、[全量竞态](docs/validation/2026-10-26-operations-go-race.json) `go test -race -count=1 ./...` 和 [vet](docs/validation/2026-10-26-operations-vet.json)均通过，未复用测试结果缓存。[Linux 五包运行报告](docs/validation/2026-10-25-operations-linux-tests.json)覆盖 state、entry、shared、runtime 和 process，包括 16 组完整网关强杀恢复。运行用户为 UID 65534，网络隔离，临时状态目录位于可执行 tmpfs；这不代表目标磁盘或物理断电验收。
- 100 并发[首轮](docs/validation/2026-10-25-operations-concurrency-100-run1.json)完成 13298 次请求；[最终独立复测](docs/validation/2026-10-26-operations-concurrency-100-final.json)完成 HTTP 11417 次、SSE 4327 次，均全部成功、无重复上游执行、无队列超时/拒绝，A→B→C 完成、服务进程最多两个。最终 HTTP / SSE 的 p95 为 156.911 / 374.514 ms，候选分配屏障最长暂停为 928.091 / 1039.480 ms；快照为 14483978 / 14812238 字节，合计峰值 RSS 555404 KiB、FD 397、CPU 228.125%（单核记 100%）。
- 最终负载仍为非 root Linux arm64 / overlayfs、8 CPU、同容器模拟上游，HTTP 与 SSE 各以 100 客户端分阶段执行约 13 秒；测量期间未同时运行 Go 测试。[宿主计时报告](docs/validation/2026-10-26-operations-concurrency-100-timing.json)的 wall / monotonic 均为 30.823 秒，未发现此前的休眠跳变。历史遗留的 `/tmp/gateway-operations-100-final-20261026.json` 虽返回成功，但其宿主计时差约 1161 秒，不作为本次最终时延证据。不同轮次没有控制宿主负载，不能据此量化优化收益；这些结果也不代表生产容量或长期稳定性。

基线及最终封存仍有全量编码/解码/持久化成本。实际 Agent 全流程、目标存储/系统与物理断电、目标机器长期容量仍未完成。以下章节为历史验证记录。

## 100 并发升级、持久化优化与完整网关强杀恢复（2026-10-23）

续接上一轮任务，按指定的 100 并发增加隔离 Linux 负载验收。环境为 macOS arm64 / Go 1.24.4，运行验收为非 root Linux arm64 / overlayfs 容器、8 个可见 CPU；不改动既有部署。

- `go test ./...`、`go test -race ./...`、`go vet ./...` 全部通过。Linux amd64 静态构建通过；Linux arm64 产物实际运行了负载脚本，state 全包和完整网关强杀测试也在容器中通过。最后补充的空 JSON 校验另行通过 shared/runtime/process 竞态回归及 shared vet。
- 16 组真实 SIGKILL 覆盖 PREPARED、预览、SEALED、最终校验、让锁、COMMITTED、激活、提交后新写入，各先杀源或候选，再清理全部旧进程并冷启动。每组包含活动候选 SSE、资源 pin、会话与幂等创建预留；确认按持久决定选版、唯一 writer、归属不变、死亡占用清零、提交后写入保留且无上游重放。这是进程崩溃验证，不是物理断电。
- 17 组目录初始化故障覆盖创建前后，以及叶、父、根目录的打开、stat、同步和关闭边界。失败不泄漏句柄，重试重新同步已有完整目录链；同步及关闭同时失败时保留两项错误。另验证已有普通文件不被覆盖。
- 新增 `Journal.Apply`，请求分配/结果及单会话变化只持久化相应记录。指定更新和删除属于同一个同步事务；部分写入、同步后报错、句柄失效、只读/持锁恢复和后续压缩保持原子性及其他领域数据。配置和维护仍可做完整替换。
- 共享响应改为直接复制值及独立的嵌套集合，减少快照回执复制的 JSON 往返。验证配置索引、接口 Map、资源 Slice、回执和任务 JSON 无可变别名；保留无效 JSON/时间戳拒绝规则，以及省略 Card 和 null 语义。
- 屏障新增实际暂停计数、当前/最近/累计/最长暂停微秒数，含等待已接纳操作排空。取消封存也完成计时，重复 Resume 不重复计数。候选的状态门与分配门分别测量，不再用完整 HANDOFF 窗口冒充暂停。
- 独立负载曾复现 503；[仅有记录级落盘的中间版本](docs/validation/2026-10-23-concurrency-100-before.json)在 SSE 阶段失败 89 次，分配屏障暂停 2614.479 ms。没有放宽 2 秒状态等待或 5 秒屏障预算。完成回执复制优化后，[第一轮](docs/validation/2026-10-23-concurrency-100-run1.json)、[第二轮](docs/validation/2026-10-23-concurrency-100-run2.json)与[最终复核](docs/validation/2026-10-23-concurrency-100-run3.json)均通过；最终脚本还强制核对队列无超时/拒绝、上游执行次数等于客户端请求次数。
- 三轮合计 39217 次 HTTP/SSE 请求全部成功、0 重复上游请求、0 队列超时/拒绝，六次升级完成，服务进程最多 2。HTTP p95 为 205.453–238.608 ms，SSE p95 为 367.296–404.844 ms；候选分配屏障最长 1491.339 ms。详细吞吐、延迟、CPU、RSS、FD、快照与测量范围见[实现进度](docs/SINGLE_PROCESS_IMPLEMENTATION_STATUS.md#100-并发-http--sse)。
- 实际 Hermes 再次验收通过发布前 HTTP/SSE，但 A2A 第二轮没有回忆首轮标识，尚未开始灰度；报告 `/tmp/gateway-real-agent-rollout-20261023-resumed.json` 为 passed=false。不能据此宣称真实 Agent 换版成功或将该失败归因于交接。

上述 100 HTTP 与 100 SSE 分阶段执行，每阶段约 13 秒，使用 50/250 ms 模拟上游和缩短的观察期。结果不是生产容量、200 混合并发或长期稳定性验收。连续跨进程操作增量、目标存储/系统与掉电矩阵、完整实际 Agent 换版、目标机器长期容量仍待完成；记录级落盘没有替代连续增量设计。以下章节保留此前记录。

## 完整网关自动灰度与正式 systemd 安装（2026-10-23）

环境：macOS arm64 / Go 1.24.4；隔离 Debian 12 / systemd 252.39 / Linux arm64 容器。Docker 测试无宿主目录挂载和公开端口，不改动已有服务。当前交付和未完成项见 [实现进度](docs/SINGLE_PROCESS_IMPLEMENTATION_STATUS.md)。以下早期章节按日期保留，不代表当前安装拓扑。

- 完整进程控制器覆盖真实灰度、全量新版直接入口、排空、状态/控制/MainPID 接管及恢复，默认安装移除 host/worker 角色和 --update-host。部署清单只有 initial_release 与 policy。
- 全量 `go test ./...`、`go test -race ./...`、`go vet ./...` 通过。定向回归覆盖观察期缺直接样本后 resume、交接期间 status 可读、配置 revision 权威性，以及预览 ACK 阻塞时候选业务仍成功且上游只执行一次。
- 冷启动对业务领域仍存在但 handoff 决定缺失/为空的情况拒绝重新建立基线。只读选版与持锁 writer 回归均通过；正常首装与卸载重装已重新验证。目录首次创建同步整个父目录链。
- 竞态脚本曾复现最终交接的 2 秒分配等待超时；入口镜像改为直接复制值类型 Map 与候选指针，移除用于复制的重复 JSON 编解码。新增导出与导入的别名隔离断言；第二轮重复仍超时后，候选提交复用已验证的最终镜像，持锁核对持久决定和快照全文，拒绝旧决定/内容不匹配。可写索引独立重建，保留原等待预算与请求成功要求。
- 本地完整进程脚本覆盖 A→B→C、活动候选 SSE 跨提交、旧 SSE 排空、失败回退、排空超时/resume、拒绝第三实例、COMMITTED 后拒绝回退、无样本自动回退与已提交版本冷启动。最终修复后的竞态构建连续三轮通过。
- `deploy/rollout-systemd/process.py --reuse-image --cycles 20` 通过正式安装器、非 root/凭据/READY、正常升级无 systemd 重启、安装 CLI 断开继续、失败后成功发布、主进程 SIGKILL、双实例 stop/start、卸载保留数据后重装及容器重启。
- 24 个成功交接窗口 p50/p95/p99 为 637/951/965 ms；2081 次请求延迟 8.479/19.811/110.775 ms；196 次在 HANDOFF 开始的请求为 11.776/302.271/475.755 ms。完整快照 469703–6345717 字节；合计峰值 RSS 216980 KiB、FD 46、服务网关进程最多 2。队列超时/拒绝和意外请求失败均为 0。
- 指标只代表小状态、串行 HTTP 回归负载。交接窗口包含预览与最终交接，不是精确屏障停顿；没有据此宣称生产容量、CPU 或长期稳定性通过。
- 实际 Agent 脚本使用两个隔离 Hermes 实例和原有模型配置。发布前 HTTP/SSE 会话、记忆续接、A2A context/任务/分页/查询和调用方隔离通过。候选阶段模型服务持续返回过载，停止测试；报告 passed=false，完整实际 Agent 换版不算通过。最新重试在发布前返回 Native session SSE reported failure，同样记录为失败，未放宽 SSE 断言。

连续逐操作增量、完整网关提交断点与存储/系统矩阵、真实模型换版及目标容量仍待完成。不可变产物与快照全部保留，尚未自动清理。

## 日志压缩持久化与真实进程崩溃（2026-10-22）

环境：macOS arm64、Go 1.24.4；临时目录、逐次注入的文件 I/O 错误及 8 个实际 SIGKILL 子进程。

- `go test ./...`、`go test -race ./...`、`go vet ./...` 通过；macOS 构建和 Linux amd64/arm64 静态交叉构建通过。
- 压缩保留已同步临时文件的句柄跨原子替换，目录同步后作为当前 writer，无需按路径重开；验证连续追加、旧 inode 读取者和关闭后的 FD 清理。
- 旧句柄关闭错误被忽略的回归先失败再通过。错误使原 Journal 失效并保留写锁；短写和失败清理时的关闭错误也传播给调用方。
- 14 组故障覆盖创建、写入、文件同步、替换、目录同步和关闭边界，含操作成功后才报错、目录同步结果不确定时新/旧名称存续两种结果。只读及持锁恢复保留更新、删除和其他领域记录，后续可重新压缩。
- 8 个真实子进程在创建前/后、部分/完整写入后、文件同步后、替换后、目录同步后和旧句柄关闭后被 SIGKILL。核对退出信号、inode 替换边界、锁释放及恢复结果。

真实进程脚本结果见 [实现进度](docs/SINGLE_PROCESS_IMPLEMENTATION_STATUS.md)。Docker daemon 不可连接，未执行本轮 Linux 容器/systemd 运行验收，也未执行物理断电。SIGKILL 遗留临时文件不会被恢复为状态，自动清理仍待实现；完整网关全部提交断点、连续操作增量和容量验收尚未完成。以下保留此前验证记录。

## 交接持久化故障与预算作用域（2026-10-22）

环境：macOS arm64、Go 1.24.4；临时目录、真实候选子进程、模拟上游及逐次注入的文件 I/O 错误。

- `go test ./...`、`go test -race ./...`、`go vet ./...` 通过；macOS 构建和 Linux amd64/arm64 静态交叉构建通过。
- 真实子进程回归先复现候选临时传输限额覆盖长期 Policy，再验证修正后本次传输仍受限、接管后的 Policy 和队列设置保持原值。
- 日志覆盖拒绝/部分/短写、完整写入后报错、fsync 前后报错、同步不确定时记录保留/丢失、压缩临时文件创建失败。失败句柄保留独占锁并拒绝后续事务；只读与持锁恢复选择相同完整记录，修复末尾后可继续追加。
- 快照覆盖 12 个创建/写入/文件同步/关闭/发布/目录同步故障点，包括操作完成后报错；失败不产生 SEALED，不改变冷启动选择，清理临时文件并允许 ABORT 后重新准备。
- 7 条决定转换各覆盖 5 个截断边界和完整记录损坏。完整 COMMITTED 拒绝回退，ABORTED 拒绝迟到提交，损坏日志拒绝启动，恢复期间禁止第二 owner。
- process 层验证快照存储失败后源可继续写入并重新交接；owner 不可用导致 readiness 失败，ResumeIngress 不能绕过。owner 健康检查遗漏和日志短写均先复现失败再通过。

这是可控存储故障模型，未执行物理断电或本轮 Linux/systemd 运行验收；日志压缩替换后的完整故障矩阵、连续操作增量、自动灰度控制器和容量验收仍待完成。真实进程脚本结果见 [实现进度](docs/SINGLE_PROCESS_IMPLEMENTATION_STATUS.md)。

## 候选检查点增量与交接预算（2026-10-22）

环境：macOS arm64、Go 1.24.4；临时目录、真实 TCP/Unix Socket、实际候选子进程与模拟上游。

- `go test ./...`、`go test -race ./...`、`go vet ./...` 通过；准备累计期限、最终编码和重复校验修正另行通过 process/shared/releaseprotocol 竞态检查及 vet。
- 基线和后续差量均重建隔离只读镜像，预览不得让锁或提交；基线后及最后 ACK 后的状态变化均进入最终 SEALED 镜像。实际候选 SSE 跨增量接管继续输出并释放租约，上游调用次数保持精确，无业务重放。
- 拒绝旧基线、epoch/revision 错误、同序号内容变化、摘要损坏、非法复制范围、截断、尾随载荷和超预算差量。差量超限在源让锁前 ABORT，源继续写入，新 handoff ID 可重新建立基线。
- 两个回归先复现失败再通过：最终校验重复调用错误地沿用旧基线；共享 JSON 中单字节插入导致外层 base64 大面积变化。修正后 2 MiB 合成共享内容的小变化在 256 KiB 差量预算内精确重建；这不是生产容量结果。
- 自定义队列条数/等待期限生效，候选接管后仍保留实际 Policy；屏障超时恢复源可写。重复 Follow 不重启准备期限，过期准备终止。
- macOS 构建及 Linux amd64/arm64 静态交叉构建通过。真实进程脚本结果记录于实现进度；Linux 交叉产物未在本轮执行。

这是检查点之间的合并差量，仍进行全量捕获和重建；连续操作日志、完整持久化故障矩阵、双入口自动灰度、正式安装/systemd 接管和容量验收仍未完成。开发 CLI 使用默认预算，完整部署接线尚待控制器实现。详细范围和预算见 [实现进度](docs/SINGLE_PROCESS_IMPLEMENTATION_STATUS.md)。下节保留本日此前状态交接验证记录。

## 完整网关状态交接与候选恢复（2026-10-22）

环境：macOS arm64、Go 1.24.4。使用临时数据目录、真实 loopback TCP/Unix Socket、实际候选子进程与模拟上游。

- `go test ./...`、`go test -race ./...`、`go vet ./...` 通过；最后的恢复输入校验改动另行通过 process 包竞态检查及 vet。
- 跨域快照包含共享业务、entry 分配/证据和 release journal，先持久化快照再写 SEALED；校验失败保留旧 owner。租约、回执和淘汰边界经显式 epoch 接管迁移。
- 实际候选 SSE 跨 COMMITTED 继续输出并释放租约；让锁后恢复源也保留原流与最新会话状态，后续入口仍可转发到原候选。
- 分别丢弃验证、提交和激活回执：验证失败恢复旧 owner；提交/激活回执丢失经原私有凭据核对持久决定和实际子进程身份，恢复新版激活，不回滚、不重放业务。
- 提交与源恢复并发争锁，只有一个可写运行时成功；COMMITTED 后拒绝源恢复和旧源停止新版。恢复通知取消后仍记录原候选、拒绝第三实例，允许明确重试。
- `Gateway.TransferHandoff` / `ResolveHandoff` 验证上述协调路径；HTTP 激活与整个发布 COMPLETED 分别处理，尚不宣称已完成 MainPID/控制权移交。
- macOS 构建及 `deploy/rollout-process/check.py` 通过，覆盖单 PID HTTP/鉴权、上游一次执行、SSE 停机排空与持久重启。
- Linux amd64/arm64 静态交叉构建通过，本轮未在 Linux 运行这些产物。
- `deploy/rollout-local/check.py` 通过，覆盖现有 host/worker 的 A→B→C、失败回退、长流排空、无 POST 重放和崩溃清理。

完整自动灰度仍缺有界增量追赶、双入口分配与直接入口样本、常驻发布控制器和正式安装/控制/systemd 接管。本轮未运行真实 systemd、实际 Agent 换版或容量验收；默认 install 保持现有行为。当前进度见 [实现进度](docs/SINGLE_PROCESS_IMPLEMENTATION_STATUS.md)。

## 完整网关灰度基础实现（2026-10-19）

本轮完成真实 Linux/systemd 交接原型验证，并接通 `process` 单进程开发入口和租约/回执迁移基础接口。完整状态快照、自动交接控制器和默认安装切换尚未完成；详细范围见 [完整网关灰度实现进度](docs/SINGLE_PROCESS_IMPLEMENTATION_STATUS.md)。

- `go test ./...`、`go test -race ./...`、`go vet ./...` 通过；新增回执丢失场景另行运行 shared 包竞态测试。
- macOS 本机构建及 Linux amd64/arm64 静态交叉构建通过。
- `deploy/rollout-systemd/check.py` 在独立 Debian bookworm 容器内使用真实 systemd、非 root 账户、LoadCredential、MAINPID/READY 屏障和 KillMode=mixed；A→B→C、旧 SSE 排空、提交前后崩溃、控制 socket 继承、双进程 stop/start 和容器重启均通过。该产物是身份探针，不承载完整网关业务状态。
- `deploy/rollout-process/check.py` 通过，验证实际一个服务 PID、HTTP/鉴权、一次上游执行、SSE 停机排空与同产物持久重启。
- `deploy/rollout-local/check.py` 重新通过，覆盖原 host/worker 灰度、连续升级、失败 POST 不重放、回退、稳定 worker 崩溃、host 死亡和完整清理。
- 新增 Go 回归验证本地/远程共用状态和额度、内部结果不泄露、业务结果判定、入口停止后继续提供状态、租约活动计数迁移、跨 epoch 回执、未确认释放重试、慢 discovery/超时排队及导入失败清理。

以上使用临时数据与模拟上游。未执行完整网关自动灰度、真实 Agent 换版、生产容量测量或正式 Linux 安装器验收。

## 稳定宿主与双槽位子进程（2026-10-19）

部署统一采用 host-worker，正常 2 个进程、灰度 3 个。`deployment.json` 仅包含 `infrastructure`、`initial_release` 和 `policy`。已删除部署版本字段、旧角色 CLI/systemd worker 驱动、旧单进程安装器、`rollout_data` 和旧部署迁移分支。宿主自身更新使用停机后的 `install --update-host`，业务发布仍使用 `install`。

执行环境为 macOS arm64、Go 1.24.4，使用临时目录、真实 loopback TCP/Unix Socket 和模拟上游。以下检查已在清理后的实现上执行完成。

| 检查 | 结果 |
| --- | --- |
| `go test ./...` | 全部通过，含无版本部署清单、子进程、宿主更新、控制身份和恢复回归 |
| `go test -race ./...` | 全部通过，含 HTTP/Hermes/A2A、跨 worker 集成测试和启动就绪检查 |
| `go vet ./...`、macOS `go build` | 通过，含最终就绪检查 |
| Linux amd64/arm64 静态交叉构建 | 通过，含最终就绪检查；未在 Linux 执行 |
| `deploy/rollout-local/check.py` | 统一为 host/worker 真实进程验证，通过；详见下文 |

`check.py` 在临时目录构建不同内容的 B/C 版本，实际验证正常 2 → 3 → 2、连续 A → B → C、失败候选回退、失败 POST 不重放、管理配置冻结/解冻、原 Agent 会话归属。长 SSE 阻止旧 worker 退出，排空超时进入 RECOVERY_REQUIRED、拒绝叠加发布；流结束后 resume 正常回收。业务灰度中 host PID、公共端口和共享状态 epoch 不变。

故障验证实际 kill 稳定 worker，确认宿主回收并重启；灰度期间 kill host，确认两个子进程退出，新 host 恢复旧稳定版和持久会话归属，新 epoch 生效。人工停止后没有存活子进程。Go 回归另外覆盖启动幂等、错误 PID/产物/槽位拒绝、状态租约阻止停止、失败启动回收、Unix peer UID、旧切流日志不能提前通过 readiness，以及各发布阶段/丢失回执/冲突日志的恢复。

安装测试使用模拟 systemctl 和临时文件，覆盖新安装（生成并加载无版本部署清单）、unit 恢复、重复安装、卸载保留数据、宿主更新备份、自定义 drop-in 保留、路由三文件/恢复事务/shared 原位保留，及活动服务、未结束发布、缺失配置、systemctl 查询失败/正在停止时拒绝变更。

尚未执行真实 Linux/systemd 安装、LoadCredential/服务用户权限、cgroup 清理、开机恢复或宿主更新失败演练；未执行远程部署、真实 Agent 二进制升级和生产容量测量。交叉构建及本机父进程生命管道验证不等同于这些实机验收。操作步骤见 [部署全流程](docs/DEPLOYMENT_LIFECYCLE.md)。

## 路由与管理记录拆分存储（2026-10-18）

`config.json` 仅保存存储格式、schema 和配置版本；`routes.json` 保存路由、实例及凭据，`route-logs.json` 保存现有管理 API 幂等回执和审计记录。三份文件通过持久化恢复事务一起提交，重启先完成未结束的写入。旧 schema 1/2 单文件在首次成功管理修改前原样备份后迁移，HTTP API 与共享 worker 配置结构保持兼容。

新增回归覆盖三份文件的字段和 0600 权限、路由/凭据/回执/审计重启恢复、各阶段替换失败及重试幂等、初始化中断恢复、序列化失败、文件缺失/损坏/版本不符、旧文件与拆分文件混放，以及 schema 2 备份冲突和迁移保留。安装器的协议推断测试先复现新存储被当作无路由、仅保留 HTTP 验收的问题，再验证旧 schema 1/2 与新存储均正确识别 HTTP、SSE、A2A、A2A SSE；缺失或未完成的路由存储拒绝继续升级。

| 检查 | 结果 |
| --- | --- |
| `go test ./...` | 全部通过 |
| `go test -race ./...` | 全部通过，含真实本地 TCP/Unix Socket 集成测试 |
| `go vet ./...`、`go build ./cmd/gateway` | 通过 |
| `deploy/rollout-local/check.py`，使用本次构建 | 通过；检查三份存储初始化、版本一致和审计写入，并验证 state/entry/双 worker、5% 分配、全量切流、会话归属、失败 POST 不重放、回退及消费者轮换/撤销 |
| Go 格式、修改文档的本地链接、`git diff --check` | 通过 |

执行环境为 Windows 上的 WSL Ubuntu、Go 1.24.4，使用临时目录、端口与模拟上游。未执行真实 systemd 安装、远程部署或掉电试验；写入中断通过各目标路径的实际 rename 失败覆盖。灰度部署需维护更新状态服务才能使用新存储，日常 worker 发布不更新该组件。

## 同版本重复安装（2026-10-14）

`install` 使用产物 SHA-256 和控制器当前 `stable` 判断已安装版本。稳定 worker 身份/就绪与入口流量配置均吻合时直接成功返回；同版本发布或恢复中接续等待原任务，保留 rollout ID、计时窗口与策略；其他发布或 `RECOVERY_REQUIRED` 不被覆盖。等待绑定原任务和目标版本，其他任务完成不能误报本次安装成功。已失败或回退的候选版本仍可重新发布。

安装客户端先查询再决定是否提交，测试中的旧控制器拒绝任何重复 POST，以验证无需更新常驻控制器也可使用新安装行为。新控制器的 `Begin` 同样支持重复提交幂等，并在日志不可用时拒绝成功响应。

`go test ./...`、`go test -race ./...`、`go vet ./...`、`git diff --check` 均通过。覆盖同内容不同文件名、停止后启动、失败后仍在服务的稳定版本、同版本正在发布、恢复中、健康/身份/协议/入口不符、等待被其他任务替换及目标版本不符、发布失败/回退、控制器重启后重试。安装测试使用临时文件、真实 Unix Socket/HTTP 和模拟 systemctl，未执行真实 Linux/systemd 安装或远程升级。

## 上游错误详情（2026-10-14）

Hermes HTTP 错误在原网关错误码外增加 `upstream.status/body/message`；A2A 保留上游 `error.code/message/data`，非协议 HTTP 错误通过 `error.data.upstream` 展示。普通 HTTP 代理连接失败附具体原因，原有上游 HTTP 状态及正文透传保持不变。新增详情对已知 Token 和明确凭据字段脱敏，`upstream.body` 保留最多 64 KiB 原始正文并标记截断；错误正文也受读取超时约束。

新增回归先在旧实现复现详情被丢弃，再验证 JSON/文本/空正文、A2A 同步及 SSE、Hermes immediate 初始错误、列表查询失败、连接拒绝、正文截断/读取中断、凭据脱敏和大整数精度。使用共享状态、Unix Socket、真实 worker HTTP 和 entry 转发验证 install 架构中的详情传递。异步首个响应与后台结束同时到达时优先返回已收到的响应；任务确认后出现上游错误仍保留未知状态。

`go test ./...`、`go test -race ./...`、`go vet ./...` 通过；最后的异步任务状态修正及错误正文空闲超时测试分别通过定向竞态回归，`git diff --check` 通过。验证使用本机模拟上游和临时存储，未部署到真实 Linux/systemd 服务。代理修复运行在 worker，已有 install 部署须用新构建二进制发布 worker 后生效。

## 安装与升级全流程文档（2026-10-13）

新增 [安装、运行文件与升级全流程](docs/DEPLOYMENT_LIFECYCLE.md)，覆盖首次安装分支、四种进程角色、配置和运行文件、版本产物与 unit 替换、请求/会话交接、全量切流、全组件维护、回退恢复及现场检查。README、配置、灰度操作和实现记录已加入入口。

流程对照安装器、systemd driver、角色启动、发布状态机、入口和持久日志实现核对。明确已有 `deployment.json` 时 `install --migrate` 仍走 worker 发布分支；整套维护采用停机备份并重建发布基线，保留业务配置和共享状态。同步修正旧配置迁移中先卸载主 unit 的步骤，保留它供安装器再次执行 stop。

16 段 Shell 示例通过 `bash -n`，YAML 示例使用项目已有 YAML 解析器检查通过；本地文档链接/锚点、空白及 `git diff --check` 检查通过，两幅 Mermaid 图按角色和交接顺序核对。本轮验证为文档检查，未执行示例中的安装、停机或文件迁移命令；Linux/systemd 整套维护演练仍待目标环境验证。

## 管理接口远程访问的测试与文档（2026-10-13）

按远程管理需求，将 `TestListenerPorts` 的管理地址预期统一为监听所有网卡。覆盖默认 `:8080` / `:9090`、不同的数据/管理自定义端口、数字和带引号的数字、1 与 65535 边界，并保留非法端口及带主机地址输入的拒绝检查。

配置示例、README、开发、使用和灰度部署文档已同步公共管理地址 `:9090`，说明通过网关可达 IP/域名和管理 Token 远程访问。内置 systemd worker 的内部监听使用显式 loopback 地址。

| 检查 | 结果 |
| --- | --- |
| `go test -count=1 ./...` | 全部通过，包含此前失败的 `internal/appconfig/TestListenerPorts` |
| `go test -race -count=1 ./...` | 全部通过 |
| `go vet ./...`、`go build ./...` | 通过 |
| Go 格式、`git diff --check`、监听说明核对 | 通过 |

以上为本机回归验证。下方目录重构章节中的监听断言失败属于本次修正前的阶段记录。

## 集成测试独立与 Options 清理（2026-10-13）

将 53 个通过公开接口验证的用例移入 [tests/integration](tests/integration/README.md)，使用独立的 `integration_test` 包。覆盖路由管理、存储恢复、HTTP/A2A/Hermes、凭据、跨 worker 请求与列表快照；资源和并发状态通过管理 API、`/metrics` 或请求结果检查，A2A/Hermes 资源经过真实数据入口创建。

`runtime` 根目录由 11 个实现文件、12 个测试文件减为 10 个实现文件、7 个测试文件。配置快照、故障注入、锁、调度额度、缓存时钟和资源内部不变量的测试保留在包内。`options.go` 中的共享模型中转别名已移除，唯一的 `Options` 结构定义合入 `runtime/gateway.go`，上层 `gateway.Options` 入口继续保留。

原有 87 个测试/基准入口全部保留；原分页测试中的过期检查拆为 `TestA2AV1ListRejectsExpiredCursor`，合计 88 个入口。6 份配置/协议 fixture 与改动前逐字节一致，A2A fixtures 随集成测试迁移。没有为测试新增生产导出接口。

| 检查 | 结果 |
| --- | --- |
| `go test ./...`、`go test -race ./...` | 网关、集成测试及其余包通过；唯一失败仍为改动前已复现的 `internal/appconfig/TestListenerPorts` |
| `GATEWAY_TEST_IN_MEMORY=true go test -race ./internal/gateway/...` | 通过，包内 HTTP 测试继续支持 net.Pipe，共享状态使用本地 Unix Socket |
| `go vet ./...`、`go build ./...` | 通过 |
| 用例清单、fixture 内容、Go 格式、`git diff --check`、本地文档链接 | 通过；集成测试不导入 runtime 或协议适配器实现包 |

既有监听断言要求 `127.0.0.1:18080`，当前 `AdminListenAddress` 返回 `:18080`；本次改动前后的失败相同。集成测试使用临时数据目录、本地随机端口和模拟上游，后台核查经过实际维护循环。

## Runtime 职责拆分（2026-10-13）

继续整理 `internal/gateway/runtime`：配置管理、资源目录、共享通信和 worker 发布逻辑分别进入 `config`、`resource`、`shared`、`rollout` 子包。根目录实现文件由 18 个减少为 11 个，保留启动组装、调度、会话、健康检查和跨模块协调。文件导航见 [Runtime 代码结构](internal/gateway/runtime/README.md)。

原有 87 个测试/基准入口全部保留，6 个独立资源测试随模块迁移；配置和协议 fixtures 逐字节一致。配置事务仍由运行时持生命周期锁，资源 Map 和目录锁封装在资源模块内；公开网关入口、HTTP/共享协议字段及存储格式沿用原有语义。

| 检查 | 结果 |
| --- | --- |
| `go test ./internal/gateway/...`、`go test -race ./internal/gateway/...` | 通过，覆盖管理/迁移/认证、资源生命周期、HTTP/SSE/A2A、跨 worker 归属与缓存 |
| `go test -race ./...` | 网关及其他包通过；唯一失败仍为既有 `internal/appconfig/TestListenerPorts` 监听地址预期差异 |
| `go vet ./...` | 通过 |
| 本机构建、Linux amd64/arm64 静态交叉构建 | 通过，产物位于临时目录 |
| `deploy/rollout-local/check.py`，使用本次构建 | state、entry、两个 worker 的真实进程/TCP 联调通过：存储自动初始化、5% 分配、全量切流、会话归属、失败 POST 不重放、回退及消费者轮换/撤销 |
| 格式、`git diff --check`、文档路径 | 通过，164 个本地文档链接有效 |

监听地址差异的原因见下一节；此次未修改监听配置。多进程检查使用临时目录、临时端口和模拟上游。

## Gateway 目录整理（2026-10-13）

`internal/gateway` 根目录保留 `gateway.go` 作为对外入口，实现按 `runtime`、`a2a`、`hermes`、`protocol` 分包。运行时组装适配器并持有配置、调度、资源和共享状态；协议适配器通过统一操作访问运行时。文件导航见 [Gateway 代码结构](internal/gateway/README.md)。

对照整理前的备份，原有 87 个测试/基准入口均保留，配置和协议 fixtures 逐字节一致。回归覆盖此前的低版本 Hermes 代理兼容、存储自动初始化，以及 HTTP/SSE/A2A、资源归属、跨 worker 列表和后台生命周期。

| 检查 | 结果 |
| --- | --- |
| `go test ./...`、`go test -race ./...` | 网关及其他包通过；`internal/appconfig/TestListenerPorts` 仍有下述既有失败 |
| `go vet ./...`、Go 格式、`git diff --check` | 通过；跨包资源和缓存结构改用显式字段初始化 |
| 本机构建、Linux amd64/arm64 静态交叉构建 | 通过；产物保存在临时目录 |
| `deploy/rollout-local/check.py`，使用本次构建 | state、entry、两个 worker 的真实进程/TCP 联调通过：自动创建存储、5% 分配、全量切换、会话归属、失败 POST 不重放、回退和消费者轮换/撤销 |
| 文档链接及移动后的 fixtures | 本地链接检查通过，协议 proto 的 SHA-256 与固定记录一致 |

全量测试的既有失败来自 [AdminListenAddress](internal/appconfig/config.go)：工作区此前已将管理监听改为所有地址（如 `:18080`），[TestListenerPorts](internal/appconfig/config_test.go) 仍要求 `127.0.0.1:18080`。该差异在本次目录整理前已复现。

本轮多进程检查使用临时目录、临时端口和模拟 HTTP 上游。真实 Hermes/官方 SDK 联调与 Linux systemd 验收分别以对应阶段记录为准。

## config.yaml 与路由上游 Token（2026-10-13）

默认配置名和 systemd 凭据路径统一为 config.yaml，示例改为 config.example.yaml。YAML credentials 与 Options.Credentials 已移除，上游 Bearer Token 通过路由接口 token 持久化和动态更新；管理响应与幂等回执脱敏。旧 credential_ref 需迁移，旧 gateway.yaml 基础组件不能直接灰度更新 worker。

验证覆盖默认配置加载、YAML 拒绝 credentials、HTTP/A2A/健康检查鉴权、Token 轮换和清除、共享 worker 使用新 Token、重启恢复、管理接口与幂等回执脱敏，以及 upsert 省略 Token 时保留原值和实例身份。`go test -race ./...`、`go vet ./...`、Python 语法、OpenAPI 引用及 `git diff --check` 检查通过。systemd 仍未进行 Linux 实机安装验证。

## 路由动态消费者（2026-10-11）

YAML callers 和静态 Options.Callers 已完全移除；消费者由路由管理 POST/upsert/PATCH 的 consumers 字段动态管理。消费者配置仅持久化 Token 哈希，管理响应及幂等回执不返回 Token/哈希；消费者 ID 作为稳定资源归属。

新增测试覆盖轮换/撤销即时生效、无消费者拒绝访问、同 Token 的路由隔离、保持原会话身份、重启恢复及幂等重放不复活旧凭据、错误输入/容量/持久化失败原子性、灰度入口和 worker 刷新、撤销后后台核查、并发轮换和在途额度不重置。YAML 严格校验测试确认 callers 字段被拒绝。

`go test ./...`、`go test -race ./...`、`go vet ./...` 全部通过。真实 state/entry/双 worker 联调已验证动态 Token 轮换后旧 Token 返回 401、新 Token 续接原 Agent、全部撤销后拒绝访问，并保留 5% 分配、全量切流及回退检查。文档 Shell/JSON 示例、链接、OpenAPI 引用和 Python 语法检查通过。本轮未改动运行中服务或实际凭据。

## YAML 运行配置（2026-10-11）

网关前台、init-only/check-ready 和四个角色统一读取 --config 指定的 YAML；默认路径现为 config.yaml。GATEWAY_* 启动变量与上游凭据环境变量已移除。当时上游凭据通过 YAML credentials 字典校验和注入；此机制已由 2026-10-13 的路由接口 token 替代。

- `go test ./...`、`go test -race ./...`、`go vet ./...` 全部通过。
- 配置回归覆盖相对路径、默认值、未知/重复字段、多文档、null、非法容量/监听、凭据错误脱敏、环境变量不覆盖/不回退，以及 --init-only 不覆盖损坏路由存储。
- 凭据回归覆盖普通 HTTP、A2A 和主动健康检查；仅在环境中存在的 credential_ref 在管理写入和重启加载时均被拒绝，Open 后凭据字典保持独立快照。
- systemd unit 改为 LoadCredential + --config，生成 YAML 权限保持 0600；模拟安装测试覆盖保留凭据、旧环境配置拒绝升级、固定内部路径校验。需要 systemd 247+，尚未进行 Linux 实机安装。
- `python3 deploy/rollout-local/check.py /tmp/aiagent-gateway-yaml-check` 通过：实际 state、entry、两个 worker 均从文件启动，验证 5% 分配、全量切换、会话归属、失败 POST 不重放及回退。
- Linux amd64 / arm64 静态交叉构建通过；Python 脚本语法、文档 Shell/JSON 和链接、OpenAPI 引用、git diff --check 通过。

未修改本机运行中的 Hermes、其凭据或系统服务，也未执行远程发布。环境变量版灰度基础组件需要维护迁移，步骤见 [YAML 配置说明](docs/CONFIGURATION.md)。

## 路由按 key 管理（2026-10-11）

本次支持 `POST /admin/v1/routes` 以正文 key 创建或更新，以及按 key GET/DELETE。更新保留未提供字段，完整实例列表按名称保持身份，实例移除/改址仍检查引用。旧内部 ID 和 PATCH 入口继续兼容。

- `go test ./...`、`go test -race ./...`、`go vet ./...` 全部通过。
- 新增回归覆盖：201/200 区分、部分更新、可选 ETag、删除、重启后幂等重放状态码、旧 schema 1 回执兼容、实例排序/权重/排空保持绑定、共享资源引用保护、失败原子性、发布冻结及并发首次创建。
- HTTP/A2A 集成验证：给已有普通聊天路由通过 POST 增加 A2A 接口，原聊天请求与新 A2A 任务请求均成功，实例 ID 保持不变，有任务时拒绝移除接口。
- 使用说明与 README 的 JSON 示例、bash/zsh 语法、本地链接检查通过；管理 OpenAPI JSON、路径参数和 operationId 唯一性检查通过。

以上为本机自动测试，未执行 Linux 安装或远程发布。灰度部署的公共管理接口由 state-server 承载；已部署环境启用本次管理功能需要更新该基础组件，不能仅灰度替换 worker。


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

按用户决定移除 0.3.0 编解码器，只支持协议 `1.0`。官方最新稳定发布核对为 `v1.0.1`（2026-05-28），schema 固定于 commit `3303592588e388e62e0f69f701af531d2f4e3991`。原始 proto、许可证、SHA-256 和派生 JSON fixtures 位于 [testdata/a2a-v1.0.1](tests/integration/testdata/a2a-v1.0.1/README.md)。本节记录当时变更；当前验收见文首 2026-10-05 记录。

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
