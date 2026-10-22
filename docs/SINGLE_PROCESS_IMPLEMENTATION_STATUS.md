# 完整网关灰度实现进度

更新日期：2026-10-22。目标与验收标准见 [实施方案](SINGLE_PROCESS_ROLLOUT_PLAN.md)。

已接通单进程基础运行时、跨域持久快照、候选检查点增量追赶、交接预算与可恢复写入权交接，验证了候选 SSE 跨提交、提交/激活回执丢失和让锁后恢复。此前已完成真实 Linux/systemd 生命周期原型。**完整网关自动灰度尚未完成；默认 install 仍采用 host + worker。**

## 当前交付

| 阶段 | 已实现和验证 | 尚未完成 |
| --- | --- | --- |
| P0 | 独占 owner 锁；PREPARED/SEALED/COMMITTED/ABORTED 决定；只读启动选择；监听 FD 继承；MainPID 与 READY 屏障；真实 systemd A→B→C | 原型是身份探针，业务状态交接需另行验收；未声明最低支持 systemd 版本 |
| P1 | `process` 开发入口；本地 dispatcher 与 UDS 共用语义；接纳与状态服务分离；候选子进程、继承 FD 和私有执行/恢复端点 | 常驻发布控制器、全量阶段新版直接入口与统一分配器、控制 listener 和正式 systemd 接管尚未合入此入口 |
| P2 | 共享/entry/release 跨域快照与持久发布；唯一 owner 锁和可写导入；有界检查点增量与精确基线校验；Policy 交接预算；租约/回执/淘汰边界迁移；提交与恢复协调；日志/快照故障注入、决定截断恢复及日志压缩进程崩溃恢复 | 按操作连续记录增量、减少全量捕获开销、完整持久化故障矩阵与容量测量 |
| P3–P5 | 沿用现有业务结果收集器与旧灰度回归测试 | 完整网关灰度闭环、默认安装切换、实际 Agent 和容量验收 |

### Linux/systemd 原型

`deploy/rollout-systemd/check.py` 在独立 Debian bookworm 容器中运行真实 systemd，使用非 root 服务账户、LoadCredential、NotifyAccess=main 和 KillMode=mixed。容器无宿主目录挂载，结束后自动清理。

已通过：

- A→B→C，三个实际不同摘要的可执行产物，正常交接不触发 systemd 重启。
- 数据、管理和 Unix 控制 listener 继承；旧进程退出不删除新版控制 socket。
- 旧 SSE 保持两实例，流结束后旧实例退出，MainPID 指向新版。
- 提交前候选死亡时恢复旧版；COMMITTED 后、MainPID 交接前故障时选择新版。
- 新主进程崩溃后重启已选版本。
- 双进程期间 systemctl stop 清理整个服务组；start 与容器重启使用持久决定选择版本。

这是生命周期原型结果，不等同于完整网关状态、正式安装器和生产系统版本矩阵验收。

### 单进程开发入口

```sh
go build -o /tmp/gateway-process ./cmd/gateway
/tmp/gateway-process process --config /path/to/development-config.yaml
```

使用独立开发数据目录。该入口由当前二进制计算产物摘要并建立 owner 基线，后续同产物冷启动恢复会话与资源。它尚未提供选择已提交产物的正式 `launch` 或在线安装入口，不能靠直接换二进制完成升级。

`internal/process/gateway.go` 按 owner 锁→配置/共享状态→业务执行器→入口的次序启动，退出时反向关闭写句柄、最后释放 owner 锁。业务通过本地 dispatcher 访问唯一状态，不走本机 HTTP；需要转发时，entry 已支持私有 UDS 目标。

本地和远程业务路径使用同一个结果收集器。本地结果以回调交给入口，不通过内部 HTTP 头或 trailer；正常 HTTP/SSE、断流、500、鉴权和取消仍采用现有判据。状态持久化不可用会使公共 readiness 失败。StopIngress 只停止新入口请求，不关闭状态服务。

### 跨域交接与候选恢复（2026-10-22）

`Gateway.PrepareHandoff` 要求源进程入口和业务排空，然后封存 entry 和共享状态，捕获配置/会话/资源/租约/回执、入口分配与结果证据、发布 journal。快照文件及目录先 fsync，随后持久化绑定源/目标身份、rollout、revision 和摘要的 SEALED 决定。重复准备同一交接返回原决定；准备失败只有在 ABORTED 写入成功后才恢复接纳。

`AdoptGateway` 和 `RestoreGateway` 分别重建候选 owner 与让锁后的源 owner。两者均先取得 owner.lock、重读决定并取得领域写锁，再按精确镜像重建。提交候选使用新 epoch；恢复源使用原 epoch 和最新封存镜像，保留候选活动租约。COMMITTED 后禁止通过源恢复入口回退。

`StartCandidate` 校验不可变产物，传递数据/管理/控制 listener、只读配置和专用 socketpair。握手绑定实际子 PID、产物摘要、incarnation、配置 revision、协议和快照格式。凭据不进入命令行或持久报告，候选状态请求限定为自己的持有者身份；当前候选公共 listener 仅在提交后激活。分块帧和总快照均有容量上限。

`Gateway.TransferHandoff` 将验证、让锁、提交、激活与异常核对串联。`ResolveHandoff` 可用新 deadline 重试；返回的 `Source` 仅表示仍持有或已重新取得写锁的源运行时，`Activated` 仅确认候选 HTTP 激活，不能据此标记整个发布 COMPLETED。

- 提交前通道 EOF 使候选退出；提交后的 EOF 不终止新版。
- 提交或激活回执丢失时，经候选原私有凭据重新核对持久决定和进程身份，再重试激活；不重发 COMMIT、不重放业务请求。
- 源恢复后，候选状态客户端显式恢复原 epoch，重试未确认释放；入口同时恢复候选的临时凭据。
- 恢复通知失败也保留候选进程记录，继续阻止第三实例；只有确认候选退出且租约清理成功后才能替换。
- 提交与回退同时争锁，只允许一方成为可写 owner。目标已提交时，旧源不能停止该进程或恢复自身写入。
- 回执淘汰边界按持有者和客户端操作序列迁移；未知旧操作拒绝重执行，新客户端使用独立序列。

真实子进程测试覆盖长 SSE 跨提交及源恢复、提交前 EOF、提交/激活/验证回执丢失、恢复通知取消、第三实例拒绝和重复恢复。共享状态测试继续覆盖租约对账与释放幂等。传输已增加下述检查点增量；以上不等同于 P2 完整故障矩阵或 P3 自动灰度验收。

### 检查点增量追赶与预算（2026-10-22）

`CandidateProcess.FollowHandoff` 首次传输只读基线，后续调用传输两个一致检查点之间的字节差量。每次捕获要求源入口和源业务已排空，短暂封存 entry/共享状态，捕获后立即恢复源接纳，再开始网络传输；候选业务仍通过源 dispatcher 访问状态。源持久决定保持 PREPARED 且不引用预览快照，预览 ACK 不允许源让锁或候选 COMMIT。

增量绑定最近一次 ACK 的 epoch、logical_revision、SHA-256 和字节数。发送端按内容分块，未变化块引用基线，其余携带原始字节；接收端先校验基线、范围、总长和预算，再在隔离缓冲区重建、检查摘要并执行完整业务不变量校验。允许合并多个已完成操作的变化，拒绝基线缺口、旧批次、revision 回退和同 revision 内容变化。不会重放业务调用、计时器或上游请求。

`TransferHandoff` 自动补齐未建立的基线，最终仍持久化完整镜像及 SEALED。最终差量必须重建该精确镜像并获得 ACK，才能让锁和提交；重复最终校验使用最新 ACK 基线。超限或校验失败终止该次准备，不自动改用超预算全量传输。新 handoff ID 可重新建立基线。

跨域镜像格式为 `gateway-process-state-3`，共享镜像直接嵌入规范 JSON，避免外层 base64 位移使小修改放大为整段变化。候选握手同时核对快照格式和 `gateway-image-delta-1`，格式不符在交接前拒绝。

`releaseprotocol.Policy.handoff` 保存预算；缺失或单项为 0 时使用默认值，负数或超上限拒绝。`OpenGatewayWithPolicy` 将预算用于源运行时，候选握手继承实际预算并在接管后配置新 dispatcher；`StartCandidate` 的快照/增量选项只能收紧该次预算。

| JSON 字段 | 默认值 | 上限 |
| --- | --- | --- |
| `prepare_timeout_seconds` | 120 秒 | 3600 秒 |
| `barrier_timeout_milliseconds` | 5000 毫秒 | 60000 毫秒 |
| `state_wait_milliseconds` | 2000 毫秒 | 60000 毫秒 |
| `state_queue_requests` | 1024 | 65536 |
| `state_queue_bytes` | 16 MiB | 1 GiB |
| `snapshot_bytes` | 256 MiB | 256 MiB |
| `delta_bytes` | 32 MiB | 256 MiB |
| `activation_timeout_seconds` | 30 秒 | 3600 秒 |

准备期限从首次 Follow 开始，重复追赶不重置；最终封存、传输、验证和提交共用屏障 deadline。激活与异常恢复共用本次激活期限，所有等待仍受调用方原 deadline 约束。超时不授予回退权，持久 COMMITTED 始终选择新版。

预算现已用于 process 交接 API；`process` 开发 CLI 使用默认预算，部署清单接线仍随完整控制器推进。当前增量是检查点差量，仍需完整捕获、解码和重建，不是每次状态操作的连续增量日志，也不保证高负载下无需等待源排空。大小上限约束编码镜像/差量和排队请求字节，不等于进程 RSS 上限。减少捕获开销、持续操作日志和容量测量仍待完成。

### 持久化故障回归与预算作用域（2026-10-22）

候选握手分别携带长期 Policy 和本次传输上限。临时收紧 `CandidateOptions.MaxSnapshotBytes` / `MaxDeltaBytes` 只限制该候选的传输，接管后保留源运行时的完整 Policy，不把临时限额延续到后续发布。真实子进程回归验证较小传输上限、接管后的 Policy 和 dispatcher 队列设置。

新增以下持久化故障回归：

- 日志拒绝写入、部分写入、短写、完整写入后报错、fsync 前后报错。错误使原句柄失效并保留写锁，后续事务不得追加；重开只使用完整且校验通过的记录。对同步结果不确定的情况分别模拟新记录保留与丢失，验证两种恢复结果。日志压缩临时文件创建失败仍保留此前已同步的事务。
- 快照父目录同步、临时文件创建/写入/同步/关闭、发布链接及快照目录同步共 12 个故障点，含操作完成后才报错。失败返回空快照描述，不产生 SEALED，不改变冷启动选择；清理临时文件后允许持久 ABORT，新交接可重新验证并同步已发布的完整镜像。
- Bootstrap、Prepare、Seal、从 PREPARED/SEALED Abort、Commit、Complete 共 7 条决定转换，各检查零字节、首字节、中段、缺最后换行和完整记录。只读选择不修复日志；持锁恢复选择相同决定并拒绝第二 owner。完整 COMMITTED 禁止回退，已恢复 ABORT 拒绝迟到候选提交，完整但损坏的末尾记录拒绝启动。
- process 层验证实际快照目录不可写入时恢复源接纳，源继续产生的状态可进入新交接快照。readiness 现在包括 owner 决定日志健康状态，手动恢复 ingress 不能绕过已关闭或失败的 owner。

上述为本地文件和可控 I/O 故障模型，未模拟物理断电。日志压缩的后续补充见下节；所有完整网关进程杀死位置及平台存储行为仍未覆盖，不能据此宣布 P2 完整故障矩阵完成。

### 日志压缩发布与崩溃恢复（2026-10-22）

`state.Journal` 在原日志事务 fsync 成功后才进行压缩。压缩镜像写入并同步后，保持同一文件句柄完成原子替换和目录同步，再切为当前写句柄；无需关闭后按路径重新打开。正常后续追加从镜像末尾继续，旧读取者仍可读取原 inode 的完整已提交前缀。

旧日志句柄关闭错误此前被忽略，现已先复现再修复：返回错误并使 Journal 失效，保留独占锁、拒绝后续事务；当前已安装句柄由 Close 负责清理。压缩短写明确失败，失败清理期间的关闭错误也保留在错误链中。

- 14 组可控故障覆盖临时文件创建、拒绝/部分/短写、写入完成后报错、文件同步前后报错、替换前后报错、目录同步前后报错、旧句柄关闭与失败清理关闭。目录同步失败分别模拟新名称保留和旧名称恢复，两者均保留此前已同步事务。
- 8 个真实子进程 SIGKILL 点位于创建前/后、部分写入后、完整写入后、文件同步后、替换后、目录同步后和旧句柄关闭后。测试核对进程确由 SIGKILL 结束、实际 inode 是否已替换，并验证原读取者、只读启动选择和持锁恢复的数据一致。
- 所有场景保留更新、删除和其他领域数据；恢复后可继续追加并再次压缩，第二 writer 始终被拒绝。正常错误路径检查文件句柄和临时路径清理。SIGKILL 遗留临时文件不参与状态选择，自动清理策略尚未实现。

以上实进程故障在 macOS arm64 执行，不等同于物理断电或 Linux 文件系统验收。本轮 Docker daemon 不可连接，未执行 Linux 容器/systemd 测试；Linux amd64/arm64 构建通过。完整网关的全部提交断点、目录首次创建持久性、平台存储矩阵和容量测量仍需继续验证。

### 已验证的状态迁移基础

`shared.Transfer` 仅包含 dispatcher epoch、逻辑序号、租约描述符和操作回执，**单独使用时不是完整业务快照**。新增 `Gateway.ExportState` 将它与共享业务镜像一起捕获。既有 dispatcher Import 仍要求一个未使用、已封存的 dispatcher；运行时必须已恢复对应会话与资源，调用方必须另行完成 owner 锁、快照校验和持久 COMMITTED 决定。新增只读 StateView 不执行这一步，也不授予写入权。

- 执行租约导入重建全局、调用方、Endpoint、接口与会话活动计数，不进行上游调用或重新分配 Agent。
- 资源 Pin 与列表并发租约按描述符重建，导入失败撤销已重建计数。
- 回执语义指纹排除 epoch；显式切换后同操作 ID 返回原租约，改变操作内容仍被拒绝。
- 未经授权的 epoch 变化继续拒绝服务；AdoptLocal 要求旧 epoch 和导入的新 owner 一致，并等待原状态调用离开。
- 释放操作在超时或回执丢失时保留不确定状态，接管后 RetryReleases 用原 ID 重试，不重放业务请求。
- dispatcher 屏障覆盖状态调用内的 discovery 与执行；排队默认上限为 1024 项、16 MiB 请求序列化字节、2 秒且不超原 deadline。完整 process 运行时可由上述 Policy 调整；超时请求恢复后不再执行。

共享业务后台维护、管理配置、直接控制和 entry/release 写入已纳入 owner 屏障与 logical_revision；各接纳操作在持久化及收尾完成后推进序号。完整网关交接必须通过上述跨域协调器，不能仅调用共享 dispatcher 的 Seal/Export。序号现已绑定检查点和差量 ACK，尚未逐操作生成增量日志。

### 共享业务状态统一屏障（2026-10-19）

`NewSharedServer` 与 owner runtime 共用 `shared.Barrier`，接纳计数覆盖完整操作和收尾，不持有业务全局锁等待上游网络。已接入：

- dispatcher 内的 discovery、状态执行与回执登记；直接 `Freeze` / `Retire` 也必须通过接纳检查，失败向发布驱动返回错误。
- 管理配置事务的持久化、配置发布和幂等回执；封存前开始读取、封存后才读完的请求体不能提交到旧 owner。
- 会话过期清理、资源维护、journal 写入、缓存淘汰、健康探测与 capability discovery、资源核查及其租约收尾。
- owner 直接业务入口和已注册的异步任务；业务执行器的上游流不占用此屏障，仍通过每次 StateAccess 操作访问 owner。

封存开始后，后台任务跳过新调度，管理写入及直接控制返回 `STATE_HANDOFF_SEALED`；状态 RPC 继续使用有界等待。Seal 等待所有已接纳操作完成，超时自动恢复接纳且禁止导出。正在 Seal 时 Resume 返回 `STATE_HANDOFF_SEAL_IN_PROGRESS`；导入/导出期间禁止并发解封。只读管理查询仍可用，Endpoint 详情不会为缺失项创建健康缓存。后台循环只允许启动一次，业务执行器不启动 owner 维护循环。

共享屏障本身只提供冻结保证，完整快照、持久提交和写锁交接由上述 process 协调器负责。旧 owner 注册的不可迁移长任务仍可能使 Seal 超时；调用方必须保留原 owner，不能据此释放写锁或宣布交接成功。子进程退出后的租约回收失败会保留记录供重试，不能跳过清理直接启动替代实例。

### 精确共享状态镜像与只读导入（2026-10-19）

`Gateway.ExportState(maxBytes)` 要求健康且已封存的共享状态 owner，在同一次 Capture 内编码配置、会话、资源目录、分页缓存、健康缓存、调度权重、活动计数、请求/失败计数和 dispatcher 租约/回执。配置保留 revision、消费者哈希、上游配置、管理幂等/审计和待备份的旧配置原文；资源保留公开 ID、Agent 归属、父子引用、别名、上游 lineage、创建指纹及幂等预留。此镜像独立于冷启动 journal，保留原始时间精度和活动引用，不采用日志中的分钟取整或计数清零。

`OpenStateView(raw, digest, maxBytes, options)` 先验证总字节数、SHA-256 和确定编码，再重建隔离的只读索引。导入不打开数据目录、锁、journal 或网络连接，不启动后台任务，也不执行过期清理。候选容量使用与正常启动相同的 Options 默认值和显式覆盖。

- 校验会话 Endpoint、资源父子链/环、别名与上游唯一性、操作指纹和引用计数。
- 用租约逐项对账全局、调用方、Endpoint、接口、会话、资源 Pin 和列表并发占用；缺失、重复或超限均拒绝整个导入。
- 配置幂等顺序/回执、dispatcher epoch/回执、分页内容及字节计数一并校验。列表正文按原始字节保存，过期时间保持不变；只读查询返回副本，不删除过期游标或延长 TTL。
- 保留 capability discovery 原接口配置，即使该字段在公共业务响应中不参与 JSON 编码；保留尚未正常淘汰的调度/健康缓存。

`shared.NewSnapshot` / `SnapshotReceiver` 提供内存中的分块传输原语：每块不超过 1 MiB，校验 snapshot_id、序号、偏移、精确长度、块摘要和最终摘要。截断不能发布镜像，坏块使接收器失效。Capture 期间接纳门保持关闭但不持其互斥锁执行编码，排队请求仍受原 deadline 约束；并发 Resume 返回 `STATE_HANDOFF_SNAPSHOT_IN_PROGRESS`。

共享镜像与只读视图现已连接上述候选通道、检查点增量、跨域持久快照和可写接管。分块上限指解码后载荷；内存镜像大小预算不等于进程峰值内存限制。连续操作增量日志、完整持久化故障矩阵和容量测量仍为 P2 未完成项。

## 本轮验证

本轮在 macOS arm64、Go 1.24.4 执行：

| 检查 | 结果 |
| --- | --- |
| `go test ./...` | 通过，包含跨域快照、源恢复、真实候选子进程、增量追赶、Policy 及持久化故障测试 |
| `go test -race ./...` | 通过，包含本轮日志压缩发布、错误传播、14 组故障边界与 8 个真实进程 SIGKILL 恢复场景 |
| `go vet ./...` | 通过 |
| macOS 构建、Linux amd64/arm64 静态交叉构建 | 通过；Linux 产物未在本轮执行 |
| `python3 deploy/rollout-process/check.py /path/to/gateway` | 单 PID HTTP、鉴权、一次上游执行、SSE 排空、持久重启通过 |
| `python3 deploy/rollout-local/check.py /path/to/gateway` | 原 host/worker 连续 A→B→C、回退、长流排空、崩溃与重启恢复通过 |

本轮构建并运行了 macOS 二进制。未重新执行真实 systemd 原型或实际 Agent/容量验收。以下保留此前基础实现的验证记录，不表示本轮重新执行。

### 此前基础实现验证

| 检查 | 结果 |
| --- | --- |
| `go test ./...` | 通过 |
| `go test -race ./...` | 通过；新增回执丢失场景另行运行 shared 包竞态测试 |
| `go vet ./...` | 通过 |
| macOS 本机构建、Linux amd64/arm64 静态交叉构建 | 通过 |
| `python3 deploy/rollout-systemd/check.py` | 上述真实 systemd 原型全部通过 |
| `python3 deploy/rollout-process/check.py /path/to/gateway` | 实际单 PID、HTTP/鉴权、一次上游执行、SSE 停机排空、持久重启通过 |
| `python3 deploy/rollout-local/check.py /path/to/gateway` | 原 host/worker 连续灰度、回退、长流、崩溃恢复全部通过 |

所有业务流量测试使用临时目录和模拟上游；本轮未执行生产部署、真实 Agent 换版或交接容量测量。

## 下一实施顺序

1. 在已实现的检查点差量基础上补充连续操作增量日志，减少全量捕获与解码开销；继续补充完整网关提交断点、目录首次创建持久性和 Linux 存储故障矩阵，并测量预算与峰值资源消耗。
2. 接入常驻发布控制器和两入口共用的唯一分配器，采集新版直接入口样本；实现 5%→全量观察→源入口排空→TransferHandoff 的自动流程。
3. 合入控制 listener 与 systemd MainPID 交接、配置解冻、连续 A→B→C；只有旧进程实际退出且新版独立可用时报告 COMPLETED。
4. 实现正式 launch、唯一新部署清单和 Type=notify 安装器，按方案移除 infrastructure、host/worker 部署分支及 --update-host，不增加部署版本或历史兼容分支。
5. 完成 Linux 安装/重启、实际 Agent、长期 SSE 与容量验收，再切换默认 install。

### 双入口分配与活动回执迁移（2026-10-22）

候选现在可在提交前显式接纳继承的公共 listener。两入口通过 owner 的同一分配器选择执行版本，记录入口 incarnation 与执行 incarnation；统计单独提供新版直接入口样本。回退流量可以通过私有凭据转发回源业务执行器，源端校验活动分配身份。管理入口在提交前访问源 owner，提交后使用本地 owner。

entry 快照携带候选入口的活动请求；交接仅要求源入口及源执行排空。候选完成回执可暂存重试，并在 COMMITTED 后写入本地 owner。分配回复失败只记录失败与清理，不重发业务请求。回执容量丢失使候选 readiness 失败。

新增真实子进程回归验证公共 HTTP 样本、新版公共 SSE 跨交接、样本和活动记录迁移、源连接断开后的独立服务，以及回退转发与三次暂停/恢复监听。这些是底层能力；自动灰度控制器及正式服务接管仍在实现。
