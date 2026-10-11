# 当前问题列表（OCR）

来源：2025-? OCR 全仓扫描（18 个文件，42 条评论）；扫描漏掉若干路径检索（`processCount|listProcesses|recv()|send(` 正则错误），因此本清单仅覆盖报告中明确列出的发现，不能视为完整审计。

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

- [ ] `examples/ping_pong.js` 接收值可能为 null；send 拒绝未处理。
- [ ] `examples/process_mgmt.js` 固定延时导致启动/回收/完成状态不确定。
- [ ] `examples/timer.js` 慢回复遗留、所有错误被误报为超时、spawn 句柄未观察。
- [ ] `examples/sandbox.js` 消息顺序依赖调度、固定延时、无界等待、未断言权限变化。
- [ ] `examples/ring.js` 实为单向链、计数少一、失败时可能无限等待。
- [ ] `tests/cli.rs` 临时目录可预测且失败不清理；stdout/stderr 分流断言不完整。
- [ ] `tests/e2e.rs` 断言失败时 worker 未必停止/join；固定 sleep 与 wall-clock 断言易抖动。
- [ ] `tests/rpc.rs` EOF/I/O 错误处理不清晰；teardown 未清理闲置客户端线程。

## P3 — 维护债务与配置缺口

- [ ] `Cargo.toml` 声明 Rust 1.85 MSRV（与 edition 2024 对齐）。
- [ ] `.gitignore` 清理重复/过宽 target/debug 规则；确认 `jevmem.config.json` 忽略策略并提供模板/说明。

## 修复记录

| 日期 | 项目 | 状态/验证 |
|---|---|---|

## 排序依据

P0 为可远程执行任意代码的安全边界；P1 为进程可能永久悬挂、状态错误或服务不可用；P2 为示例错误和测试不确定性；P3 为开发者体验与配置清晰度。

> OCR 建议需结合 API 与实现验证；尤其“send 失败”“邮箱关闭”等结论不能仅凭扫描文本直接采纳。
