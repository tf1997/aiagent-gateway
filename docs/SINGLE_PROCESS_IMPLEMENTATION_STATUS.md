# 完整网关灰度实现进度

更新日期：2026-10-19。目标与验收标准见 [实施方案](SINGLE_PROCESS_ROLLOUT_PLAN.md)。

已接通单进程基础运行时，完成真实 Linux/systemd 进程交接原型，并加入状态租约和回执迁移的基础接口。**完整网关自动灰度尚未完成；默认 install 仍采用 host + worker。**

## 当前交付

| 阶段 | 已实现和验证 | 尚未完成 |
| --- | --- | --- |
| P0 | 独占 owner 锁；PREPARED/SEALED/COMMITTED/ABORTED 决定；只读启动选择；监听 FD 继承；MainPID 与 READY 屏障；真实 systemd A→B→C | 原型是身份探针，业务状态交接需另行验收；未声明最低支持 systemd 版本 |
| P1 | `process` 开发入口；同一 PID 内入口、本地业务处理、状态 owner；本地 dispatcher 与 UDS 共用语义；入口接纳和状态服务分离；持久状态恢复 | 发布控制器、候选启动与私有进程端点尚未合入此入口 |
| P2 | 可序列化租约；dispatcher 屏障与有界等待；租约/回执导入导出；显式 AdoptLocal；未确认释放保留原操作 ID 重试 | 完整快照/增量、全部后台写入屏障、回执淘汰边界、与 owner 提交记录联动及完整故障矩阵 |
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

`shared.Transfer` 当前仅包含 dispatcher epoch、逻辑序号、租约描述符和操作回执，**不是完整业务快照**。导入要求一个未使用、已封存的 dispatcher；运行时必须已恢复对应会话与资源，调用方必须另行完成 owner 锁、快照校验和持久 COMMITTED 决定。

- 执行租约导入重建全局、调用方、Endpoint、接口与会话活动计数，不进行上游调用或重新分配 Agent。
- 资源 Pin 与列表并发租约按描述符重建，导入失败撤销已重建计数。
- 回执语义指纹排除 epoch；显式切换后同操作 ID 返回原租约，改变操作内容仍被拒绝。
- 未经授权的 epoch 变化继续拒绝服务；AdoptLocal 要求旧 epoch 和导入的新 owner 一致，并等待原状态调用离开。
- 释放操作在超时或回执丢失时保留不确定状态，接管后 RetryReleases 用原 ID 重试，不重放业务请求。
- dispatcher 屏障覆盖状态调用内的 discovery 与执行；排队上限目前固定为 1024 项、16 MiB 请求序列化字节、2 秒且不超原 deadline。超时请求恢复后不再执行。

这些预算尚未接入发布 Policy，后台维护、管理配置写入和直接控制方法也尚未统一进入交接协调器；因此不能用单次 Seal/Export 宣称完整状态已冻结。完整快照还须包含列表内容、精确 TTL/活动引用、健康与调度状态、持久配置/领域数据、结果证据，以及回执淘汰边界。

## 本轮验证

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

1. 完成完整状态镜像与只读导入/增量重放，校验摘要、容量和引用不变量；使后台 discovery、维护、核查和管理写入在屏障后不再落到旧 owner。
2. 把快照、租约/回执、候选身份和 handoff 决定组合成一个可恢复协议，覆盖提交/fsync/回执丢失与回退并发，持久化回执淘汰边界。
3. 合入候选进程、统一分配器、直接入口样本、监听/控制交接和配置解冻；只有旧进程退出且新版独立可用时报告 COMPLETED。
4. 实现正式 launch、唯一新部署清单和 Type=notify 安装器，按方案移除 infrastructure、host/worker 部署分支及 --update-host，不增加部署版本或历史兼容分支。
5. 完成 Linux 安装/重启、实际 Agent、长期 SSE 与容量验收，再切换默认 install。
