# 当前问题列表（OCR）

来源：2025-? OCR 全仓扫描（18 个文件，42 条评论）。补扫（会话 `2fa66585`，42 条）已完成，结论与待决策项见文末“补扫分诊”。

## P0 — 立即处理

- [x] **未经认证的 RPC 暴露特权操作**（安全，高）：`src/main.rs` 默认拒绝非 loopback RPC 监听。
- [x] **loopback 下无认证（已决策：文档化信任模型）**（安全，critical，补扫 `src/rpc.rs:39`）：本机任何进程可连 RPC 并 `spawn` `PRIVILEGED`。README 已写明信任边界；RPC 默认关闭，仅 `--rpc-port` 开启时可用。若要在多用户主机上使用，再重评方案 ①。


## P1 — 进程生命周期、并发与服务健壮性

- [x] `src/main.rs` 先停止并 join RPC，再停止并 join workers，避免关停期间 RPC 请求遗留进程。
- [x] `src/scheduler.rs` kill 保持终态优先级，`fail` 不覆盖 `Killed`。
- [x] `src/scheduler.rs` 进程创建记账与注册失败回滚协调，避免 active 计数丢失/重复。
- [x] `src/scheduler.rs` 队列发送失败时回收无法重新入队的进程。
- [x] `src/process.rs` 初始化失败移除注册，并回滚 active 记账。
- [x] `src/process.rs` `recv(timeoutMs)` deadline 有界构造，避免超大值触发 `Instant` 溢出。
- [x] `src/process.rs` 错误类名通过固定白名单选择，不接受动态源码注入。
- [x] `src/rpc.rs` 限制连接数量和读写时长；线程创建/套接字配置错误受控处理，关停时关闭并 join handlers。
- [x] `src/rpc.rs` 校验 JSON-RPC envelope，非法请求返回 `-32600`；无效 ID 的错误响应使用 `null`。
- [x] `src/scheduler.rs` 提供原子 `rename_if_alive`，RPC 不再 check-then-act。
- [x] `src/main.rs` worker/RPC 线程创建失败会报告并清理已启动 worker。

修复提交：`7f62860`、`46b1a28`、`5768a79`。验证：`cargo test`（52 项）、`cargo clippy --all-targets`、`cargo fmt --check`。

- [ ] **spawn 权限快照过早**（安全，高，已核实 `src/process.rs:238-250`）：`opts` 的 getter 能运行用户 JS 并收窄自身权限，随后仍按旧快照派生特权子进程。修法：解析 `opts` 之后重新读取并检查。
- [ ] **recv 持锁跨越 resolve**（中，已核实 `src/process.rs:287`）：`match` 的守卫持有到分支结束；JSON 对象的 `then` getter 同步执行，可在守卫内重入 `recv` 导致 worker 死锁，殃及同 worker 的其他进程。修法：先取出结果、释放守卫，再 resolve。
- [ ] **RPC 慢速客户端占线程**（高，已核实 `src/rpc.rs:130`）：`read_timeout` 按单次读计，客户端每 59 秒送 1 字节即可长期占线程。修法：为整行读取设总截止时间。
- [ ] **RPC 绑定失败仍以 0 退出**（中，已核实 `src/main.rs:115-119`）：绑定失败只打印，进程按“未请求 RPC”继续。修法：绑定失败时以退出码 1 结束。
- [x] **单个 job 无中断（已实现可选上限）**：`--max-job-ms MS`，默认关闭。超限即让进程失败；实测 `try/catch` 无法捕获该中断。`tests/e2e.rs::job_over_budget_fails_the_process` 覆盖 `run_slice` 路径；创建时入口脚本路径仅手测（无自动化测试）。
- [ ] **ERROR_DESCRIBER 可抛错**（高，结构确认，未实测，`src/process.rs:152-155`）：`String(e)` 或 `e.stack` 抛错时 `__otter_error` 不会被调用，进程可能停在非终态。修法：描述函数包 try/catch，兜底返回固定文本。
- [ ] **投递与超时竞态**（中，已核实逻辑，`src/scheduler.rs:185-197`、`:579`）：消息在截止前入箱，但投递线程在取 `parked` 锁前被挂起，timer 先超时。消息不丢、仍留在 inbox，但本次 `recv` 误报超时。修法待定。
- [ ] **main 等待循环不监视线程死亡**（中，待核实，`src/main.rs:124-141`）：持久模式下 RPC 线程若崩溃，进程可能永久挂起。
- [ ] **关停阻塞顺序**（中，待核实，`src/main.rs:142-144`）：RPC join 先于 worker 停止；RPC 线程若被阻塞，关停无限期等待。

## P2 — 示例与测试可靠性

- [ ] `examples/ping_pong.js`：未改。`send` 不返回失败（docs/api.md：发往已结束 pid 静默丢弃），`recv()` 仅在收到 `null` 消息时返回 `null`，协议中父进程从不发送 `null` 给子进程，因此扫描结论不成立。
- [x] `examples/process_mgmt.js` 去除启动固定延时；杀死后轮询至回收（有界 500 ms）；幸存者确认后再打印 `done`。
- [x] `examples/timer.js` 非 `TimeoutError` 异常重新抛出。慢回复的生命周期未改（进程结束时邮箱随之丢弃）；未观察的 spawn 句柄未改。
- [x] `examples/sandbox.js` 去除固定延时（spawn 返回时子进程已挂起）；对 narrow 结果断言。消息顺序由同步 spawn 决定，非竞态；无界 `recv` 未改。
- [x] `examples/ring.js` 计数改为“经过的 worker 数”；`recv` 加 10 秒超时，worker 异常退出时报告 token 丢失。
- [x] `tests/cli.rs` 唯一临时目录 + `Drop` 清理；stderr 断言不得包含 stdout 文本（提交 0a36133）。
- [x] `tests/e2e.rs` worker 断言失败时仍停止并 join（提交 0a36133）；“let it park”固定延时删除（spawn 同步挂起）；kill/自杀回收改为 2 秒有界轮询；唤醒上限放宽为 10 秒。未改：`busy_queue_does_not_starve_sleep` 的 1 秒饥饿阈值仍是 wall-clock 断言。验证：串行 15 次与 4 路并发 ×5 轮全部通过。
- [x] `tests/rpc.rs` EOF 作为错误报告；通知测试只接受读超时；teardown 前关闭闲置客户端（提交 0a36133）。
- [ ] **测试失败路径清理**（补扫，中）：`tests/e2e.rs:78-81` 工作线程仅在成功路径停止，应统一用 `WorkerGuard`；`tests/e2e.rs:44-46` 丢弃 join 结果；`:45` 无界 join 可能绕过完成超时。`tests/rpc.rs:17-19` 失败路径不清理，`:53-55` 丢弃 join 结果，`:357-358` join 无期限；`:412-415` 发送超大请求时无写超时。
- [ ] **测试断言过宽**（补扫，中）：`tests/rpc.rs:364-367` 任何 `request_raw` 错误都满足断言；`:82` 不校验 JSON-RPC envelope；`tests/cli.rs:43`、`:49` 负向测试接受信号/abort，且缺失路径可能真实存在。
- [ ] **示例缺失路径**（补扫，中）：`examples/process_mgmt.js:10` worker 创建与 ack 无失败路径；`examples/ring.js:22` 超时只覆盖根 recv；`examples/timer.js:5` 两个 pid 未使用。
- [ ] **e2e 时序假设与命名**（补扫，中）：`tests/e2e.rs:245`、`:283`、`:349`、`:537` 依赖 wall-clock 或后续动作；`:639`、`:752` 测试名与内容不符（未观察子进程的 sandbox）。

## P3 — 维护债务与配置缺口

- [x] `Cargo.toml` 声明 Rust 1.85 MSRV（与 edition 2024 对齐）。
- [x] `.gitignore` 已只保留 `/target`，无过宽 `debug` 规则。`jevmem.config.json` / `JEVMEM.md` 故意不入库：本地工具配置，仓库内无代码或文档读取它（提交 51c01fb）。
- [ ] **已知权衡待注释**（补扫，低）：`src/scheduler.rs:475` 的 `recv(timeout)` 定时器不随消息取消（惰性删除，`CLAUDE.md` 已说明），需补 `shortcut:` 注释，说明何时改为可删除的堆。
- [ ] `src/rpc.rs:164`：接受数组 `params`，但所有方法都不使用（低）。
- [ ] `src/main.rs:66`：入口脚本先于 RPC 监听启动（待核实，设计取舍，低）。
- [ ] `src/process.rs:175-191`：生命周期钩子（`__otter_done` / `__otter_error`）是用户代码可见的全局，脚本可伪造本进程终态或覆盖 resolver。影响仅限本进程自身，不构成提权（scan 标为高，此处判为中）。
- [ ] `src/rpc.rs:176-182`：`shutdown` 先回包后置位关停标志，属预期顺序，仅记录（低）。
- [ ] `src/main.rs:106`：端口为 0 时打印的是 `0`，应打印 `local_addr()`（低）。
- [ ] README 第 63 行仍写“非 loopback 打印警告”，代码已拒绝启动，需同步文档（低）。

## 修复记录

| 日期 | 项目 | 状态/验证 |
|---|---|---|

## 排序依据

P0 为可远程执行任意代码的安全边界；P1 为进程可能永久悬挂、状态错误或服务不可用；P2 为示例错误和测试不确定性；P3 为开发者体验与配置清晰度。

> OCR 建议需结合 API 与实现验证；尤其“send 失败”“邮箱关闭”等结论不能仅凭扫描文本直接采纳。

## 补扫分诊（会话 `2fa66585`，42 条）

- **→ 结论：** 补扫已完成。17 个文件（其中 6 个复用既有结果），42 条评论，已全部归入上方 P0–P3。会话 `2d62b71e` 已中止（7 个文件、8 条），未采用。
- **→ 原扫描缺口：** 正则检索 `processCount|listProcesses|recv()|send(` 失败；补扫为文件级审阅，已覆盖这些调用点所在的文件。
- **→ 不改（设计如此）：** `send` 对死 pid 返回 `true`（`docs/api.md` 定义为静默丢弃）；定时器惰性删除（`CLAUDE.md` 已说明的权衡）。
- **→ 决策已定：** 见下节“已决策：方案比较”。

## 已决策：方案比较

**前提：** RPC 默认关闭（需 `--rpc-port`），非 loopback 已拒绝启动。单个 job 无中断的影响是“占住一个 worker”。

### A. loopback RPC 认证

| 方案 | 改动 | 能防住什么 | 代价 |
|---|---|---|---|
| ① 令牌认证 | 中：启动时生成随机令牌，首个请求必须带上，常量时间比较 | 其他系统用户 | 标准库无随机源，需读 `/dev/urandom` 或加依赖；同一用户的进程仍能读令牌文件 |
| ② 只读 | 小：去掉 `spawn` / `send` / `shutdown` | 任意代码执行 | README 第 52–55 行的示例全部失效，RPC 只剩观察价值 |
| ③ 文档化信任模型 | 零代码 | 无（只声明边界） | 多用户主机上其他用户仍可控制；需在 README 写明“仅限单用户开发机” |

**→ 推荐 ③。** 同一用户的进程本来就能读令牌文件，① 只挡得住“其他系统用户”，在单用户开发机上几乎没有额外保护。若 otter 会在多用户主机上使用，再选 ①。

未列入的第四个选项：Unix 域套接字 + 0600 权限，能挡住其他用户且不需令牌；但会改掉 README 中的 TCP/`nc` 用法，且只支持 Unix。

### B. 单个 job 无中断

| 方案 | 改动 | 效果 | 代价 |
|---|---|---|---|
| 固定时间片上限 | 小：用 `set_interrupt_handler` 计时，超限即中断 | 死循环进程被终止，worker 释放 | 默认上限会误杀长计算；中断后进程失败、无法恢复（QuickJS 无法在中途挂起，需实测确认） |
| 可选上限（`--max-job-ms`，默认不限） | 小：多一个 CLI 参数 | 需要时开启，默认行为不变 | 默认仍可被冻结 |
| 保持不变并文档化 | 零代码 | 无 | 任一进程写死循环即占住一个 worker；全部 worker 被占则运行时停摆 |

**→ 推荐“可选上限”。** 它给需要防护的场景一个开关，又不误杀默认的长计算。

**→ 结果：** A 选 ③（文档化信任模型）；B 选“可选上限”，已实现为 `--max-job-ms`（默认关闭）。
