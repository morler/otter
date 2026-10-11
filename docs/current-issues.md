# 当前问题列表（OCR）

来源：2025-? OCR 全仓扫描（18 个文件，42 条评论）；扫描中一次路径检索因正则错误失败（`processCount|listProcesses|recv()|send(`），因此本清单只覆盖报告明确列出的发现，尚未是完整审计。缺口与补扫计划见文末“未覆盖范围”。

## P0 — 立即处理

- [x] **未经认证的 RPC 暴露特权操作**（安全，高）：`src/main.rs` 默认拒绝非 loopback RPC 监听。


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

## P2 — 示例与测试可靠性

- [ ] `examples/ping_pong.js`：未改。`send` 不返回失败（docs/api.md：发往已结束 pid 静默丢弃），`recv()` 仅在收到 `null` 消息时返回 `null`，协议中父进程从不发送 `null` 给子进程，因此扫描结论不成立。
- [x] `examples/process_mgmt.js` 去除启动固定延时；杀死后轮询至回收（有界 500 ms）；幸存者确认后再打印 `done`。
- [x] `examples/timer.js` 非 `TimeoutError` 异常重新抛出。慢回复的生命周期未改（进程结束时邮箱随之丢弃）；未观察的 spawn 句柄未改。
- [x] `examples/sandbox.js` 去除固定延时（spawn 返回时子进程已挂起）；对 narrow 结果断言。消息顺序由同步 spawn 决定，非竞态；无界 `recv` 未改。
- [x] `examples/ring.js` 计数改为“经过的 worker 数”；`recv` 加 10 秒超时，worker 异常退出时报告 token 丢失。
- [x] `tests/cli.rs` 唯一临时目录 + `Drop` 清理；stderr 断言不得包含 stdout 文本（提交 0a36133）。
- [x] `tests/e2e.rs` worker 断言失败时仍停止并 join（提交 0a36133）；“let it park”固定延时删除（spawn 同步挂起）；kill/自杀回收改为 2 秒有界轮询；唤醒上限放宽为 10 秒。未改：`busy_queue_does_not_starve_sleep` 的 1 秒饥饿阈值仍是 wall-clock 断言。验证：串行 15 次与 4 路并发 ×5 轮全部通过。
- [x] `tests/rpc.rs` EOF 作为错误报告；通知测试只接受读超时；teardown 前关闭闲置客户端（提交 0a36133）。

## P3 — 维护债务与配置缺口

- [x] `Cargo.toml` 声明 Rust 1.85 MSRV（与 edition 2024 对齐）。
- [x] `.gitignore` 已只保留 `/target`，无过宽 `debug` 规则。`jevmem.config.json` / `JEVMEM.md` 故意不入库：本地工具配置，仓库内无代码或文档读取它（提交 51c01fb）。

## 修复记录

| 日期 | 项目 | 状态/验证 |
|---|---|---|

## 排序依据

P0 为可远程执行任意代码的安全边界；P1 为进程可能永久悬挂、状态错误或服务不可用；P2 为示例错误和测试不确定性；P3 为开发者体验与配置清晰度。

> OCR 建议需结合 API 与实现验证；尤其“send 失败”“邮箱关闭”等结论不能仅凭扫描文本直接采纳。

## 未覆盖范围（待补扫）

- [ ] **缺口：** 扫描中 `processCount|listProcesses|recv()|send(` 的 Perl 正则未能执行（`recv()` 括号未闭合），因此对这些 API 调用点的审查没有完成。
- [ ] **计划：** 运行一次新的 `ocr_scan`（全仓，`timeout_sec` 1200）；把结果中新增且未在本清单的发现追加到对应优先级，并记录扫描会话 ID。
- [ ] **结果：** 待扫描完成后回填。
