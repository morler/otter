# 当前问题列表（OCR）

来源：2025-? OCR 全仓扫描（18 个文件，42 条评论）；扫描漏掉若干路径检索（`processCount|listProcesses|recv()|send(` 正则错误），因此本清单仅覆盖报告中明确列出的发现，不能视为完整审计。

## P0 — 立即处理

- [ ] **未经认证的 RPC 暴露特权操作**（安全，高）：`src/main.rs` 非 loopback 监听允许远程 spawn/kill/send/shutdown。拒绝非 loopback 默认配置，除非增加明确安全机制。

- [x] **未经认证的 RPC 暴露特权操作**：`src/main.rs` 现在拒绝非 loopback RPC；已通过 `cargo test`。

## P1 — 进程生命周期、并发与服务健壮性

- [ ] `src/main.rs` 先停 worker 后 join RPC，造成关停竞态；需先停 RPC 接入/请求再停 worker。
- [ ] `src/scheduler.rs` `fail` 可覆盖 `Killed`；保持 kill 优先级。
- [ ] `src/scheduler.rs` 创建进程对外可见早于 `active` 计数；协调注册、计数与失败回滚。
- [ ] `src/scheduler.rs` 队列发送失败会丢弃进程；统一确保失败队列项被回收。
- [ ] `src/process.rs` 初始化/脚本执行失败时残留注册表条目。
- [ ] `src/process.rs` 超大 `recv(timeoutMs)` 可使 `Instant` 加法溢出 panic。
- [ ] `src/process.rs` 错误类名插入 JS 源码，需白名单/安全构造。
- [ ] `src/rpc.rs` 无写超时、线程创建 panic；连接可耗尽线程池，资源失败可终止服务。
- [ ] `src/rpc.rs` JSON-RPC 请求校验宽松；非法请求应返回 `-32600`。
- [ ] `src/rpc.rs` rename 的存活检查与改名有竞态，需原子 API。
- [ ] `src/main.rs` worker/RPC 线程创建失败使用 `expect`，需受控清理与退出。

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
