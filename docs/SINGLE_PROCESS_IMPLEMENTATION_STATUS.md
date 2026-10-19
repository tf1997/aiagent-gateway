# 完整网关灰度实现进度

更新日期：2026-10-19。目标与验收标准见 [实施方案](SINGLE_PROCESS_ROLLOUT_PLAN.md)。

已接通单进程基础运行时，完成真实 Linux/systemd 进程交接原型，并加入状态租约、回执迁移、共享业务状态统一写入屏障，以及精确共享状态镜像、只读导入和分块校验。**完整网关自动灰度尚未完成；默认 install 仍采用 host + worker。**

## 当前交付

| 阶段 | 已实现和验证 | 尚未完成 |
| --- | --- | --- |
| P0 | 独占 owner 锁；PREPARED/SEALED/COMMITTED/ABORTED 决定；只读启动选择；监听 FD 继承；MainPID 与 READY 屏障；真实 systemd A→B→C | 原型是身份探针，业务状态交接需另行验收；未声明最低支持 systemd 版本 |
| P1 | `process` 开发入口；同一 PID 内入口、本地业务处理、状态 owner；本地 dispatcher 与 UDS 共用语义；入口接纳和状态服务分离；持久状态恢复 | 发布控制器、候选启动与私有进程端点尚未合入此入口 |
| P2 | 可序列化租约；共享业务状态统一屏障与有界等待；精确共享镜像/只读导入/1 MiB 分块校验；租约/回执迁移；显式 AdoptLocal；未确认释放保留原操作 ID 重试 | 统一增量序列、entry/release 全域镜像与交接协调、候选通道与持久快照、回执淘汰边界、与 owner 提交记录联动及完整故障矩阵 |
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

### 已验证的状态迁移基础

`shared.Transfer` 仅包含 dispatcher epoch、逻辑序号、租约描述符和操作回执，**单独使用时不是完整业务快照**。新增 `Gateway.ExportState` 将它与共享业务镜像一起捕获。既有 dispatcher Import 仍要求一个未使用、已封存的 dispatcher；运行时必须已恢复对应会话与资源，调用方必须另行完成 owner 锁、快照校验和持久 COMMITTED 决定。新增只读 StateView 不执行这一步，也不授予写入权。

- 执行租约导入重建全局、调用方、Endpoint、接口与会话活动计数，不进行上游调用或重新分配 Agent。
- 资源 Pin 与列表并发租约按描述符重建，导入失败撤销已重建计数。
- 回执语义指纹排除 epoch；显式切换后同操作 ID 返回原租约，改变操作内容仍被拒绝。
- 未经授权的 epoch 变化继续拒绝服务；AdoptLocal 要求旧 epoch 和导入的新 owner 一致，并等待原状态调用离开。
- 释放操作在超时或回执丢失时保留不确定状态，接管后 RetryReleases 用原 ID 重试，不重放业务请求。
- dispatcher 屏障覆盖状态调用内的 discovery 与执行；排队上限目前固定为 1024 项、16 MiB 请求序列化字节、2 秒且不超原 deadline。超时请求恢复后不再执行。

这些预算尚未接入发布 Policy。共享业务状态的后台维护、管理配置写入和直接控制方法已使用同一屏障，具体范围见下节；entry/release 领域及 owner 提交记录仍未合入交接协调器，因此不能用单次 Seal/Export 宣称整个网关状态已冻结。新增镜像已包含列表内容、精确 TTL/活动引用、健康与调度状态、共享配置和资源幂等；entry 的流量归属/结果证据、发布控制数据和回执淘汰边界仍待接入。当前 logical_revision 仍只覆盖 dispatcher 与直接控制操作，尚未形成包含管理和后台变化的统一增量序列。

### 共享业务状态统一屏障（2026-10-19）

`NewSharedServer` 与 owner runtime 共用 `shared.Barrier`，接纳计数覆盖完整操作和收尾，不持有业务全局锁等待上游网络。已接入：

- dispatcher 内的 discovery、状态执行与回执登记；直接 `Freeze` / `Retire` 也必须通过接纳检查，失败向发布驱动返回错误。
- 管理配置事务的持久化、配置发布和幂等回执；封存前开始读取、封存后才读完的请求体不能提交到旧 owner。
- 会话过期清理、资源维护、journal 写入、缓存淘汰、健康探测与 capability discovery、资源核查及其租约收尾。
- owner 直接业务入口和已注册的异步任务；业务执行器的上游流不占用此屏障，仍通过每次 StateAccess 操作访问 owner。

封存开始后，后台任务跳过新调度，管理写入及直接控制返回 `STATE_HANDOFF_SEALED`；状态 RPC 继续使用有界等待。Seal 等待所有已接纳操作完成，超时自动恢复接纳且禁止导出。正在 Seal 时 Resume 返回 `STATE_HANDOFF_SEAL_IN_PROGRESS`；导入/导出期间禁止并发解封。只读管理查询仍可用，Endpoint 详情不会为缺失项创建健康缓存。后台循环只允许启动一次，业务执行器不启动 owner 维护循环。

这些是共享业务状态的冻结保证，并非完整快照、持久提交或写锁交接。旧 owner 注册的不可迁移长任务仍可能使 Seal 超时；调用方必须保留原 owner，不能据此释放写锁或宣布交接成功。子进程退出后的租约回收失败会保留记录供重试，不能跳过清理直接启动替代实例。

### 精确共享状态镜像与只读导入（2026-10-19）

`Gateway.ExportState(maxBytes)` 要求健康且已封存的共享状态 owner，在同一次 Capture 内编码配置、会话、资源目录、分页缓存、健康缓存、调度权重、活动计数、请求/失败计数和 dispatcher 租约/回执。配置保留 revision、消费者哈希、上游配置、管理幂等/审计和待备份的旧配置原文；资源保留公开 ID、Agent 归属、父子引用、别名、上游 lineage、创建指纹及幂等预留。此镜像独立于冷启动 journal，保留原始时间精度和活动引用，不采用日志中的分钟取整或计数清零。

`OpenStateView(raw, digest, maxBytes, options)` 先验证总字节数、SHA-256 和确定编码，再重建隔离的只读索引。导入不打开数据目录、锁、journal 或网络连接，不启动后台任务，也不执行过期清理。候选容量使用与正常启动相同的 Options 默认值和显式覆盖。

- 校验会话 Endpoint、资源父子链/环、别名与上游唯一性、操作指纹和引用计数。
- 用租约逐项对账全局、调用方、Endpoint、接口、会话、资源 Pin 和列表并发占用；缺失、重复或超限均拒绝整个导入。
- 配置幂等顺序/回执、dispatcher epoch/回执、分页内容及字节计数一并校验。列表正文按原始字节保存，过期时间保持不变；只读查询返回副本，不删除过期游标或延长 TTL。
- 保留 capability discovery 原接口配置，即使该字段在公共业务响应中不参与 JSON 编码；保留尚未正常淘汰的调度/健康缓存。

`shared.NewSnapshot` / `SnapshotReceiver` 提供内存中的分块传输原语：每块不超过 1 MiB，校验 snapshot_id、序号、偏移、精确长度、块摘要和最终摘要。截断不能发布镜像，坏块使接收器失效。Capture 期间接纳门保持关闭但不持其互斥锁执行编码，排队请求仍受原 deadline 约束；并发 Resume 返回 `STATE_HANDOFF_SNAPSHOT_IN_PROGRESS`。

当前输出是**共享业务状态镜像和只读验证视图**，尚未连接候选进程通道、快照文件/目录 fsync、owner 决定或可写 runtime 激活。分块上限指解码后载荷；内存镜像大小预算不等于进程峰值内存限制，尚未完成容量测量。统一增量重放、跨域完整快照和提交恢复继续列为 P2 未完成项。

## 本轮验证

本轮在 Windows 工作区通过 WSL2 Ubuntu、Go 1.26.2 执行 Linux 测试：

| 检查 | 结果 |
| --- | --- |
| `go test ./...` | 通过，新增精确镜像、只读导入、引用/租约对账、容量、坏块/截断/摘要、Capture 期间 deadline 测试 |
| `go test -race ./...` | 通过；分块→StateView 串联测试另行通过 runtime 包竞态测试 |
| `go vet ./...` | 通过 |
| `python3 deploy/rollout-process/check.py /path/to/gateway` | 单 PID HTTP、鉴权、一次上游执行、SSE 排空、持久重启通过 |
| `python3 deploy/rollout-local/check.py /path/to/gateway` | 原 host/worker 连续 A→B→C、回退、长流排空、崩溃与重启恢复通过 |

本轮未重新执行真实 systemd 原型、macOS 构建或实际 Agent/容量验收。以下保留此前基础实现的验证记录，不表示本轮重新执行。

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

1. 在已验证共享业务镜像之上统一记录管理和后台变化的 logical_revision，实现有界增量重放；把 entry/release 领域、快照持久化和候选通道纳入同一交接协调器。
2. 把快照、租约/回执、候选身份和 handoff 决定组合成一个可恢复协议，覆盖提交/fsync/回执丢失与回退并发，持久化回执淘汰边界。
3. 合入候选进程、统一分配器、直接入口样本、监听/控制交接和配置解冻；只有旧进程退出且新版独立可用时报告 COMPLETED。
4. 实现正式 launch、唯一新部署清单和 Type=notify 安装器，按方案移除 infrastructure、host/worker 部署分支及 --update-host，不增加部署版本或历史兼容分支。
5. 完成 Linux 安装/重启、实际 Agent、长期 SSE 与容量验收，再切换默认 install。
