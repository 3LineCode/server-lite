# pyline 重构主计划（已批准）

原 `ServerLite`/`frameaio`（约 7000 行）作为行为规格与差分测试 oracle，本仓库绿地重写。
四项已确认的架构决策：

1. **热更**：保留"对象地址不变"的就地热更全套能力，加三重防护网（沙箱预验证 /
   Python 版本兼容矩阵 / 全程结构化日志与校验和）。
2. **IPC**：纯 ZeroMQ 消息总线（ROUTER/DEALER），彻底删除共享内存。
3. **API**：对业务全面 async/await，不做回调兼容层。
4. **Python**：3.12 基线，CI 跑 3.12/3.13 双矩阵。

## 阶段划分

| 阶段 | 内容 | 交付 |
|---|---|---|
| P0 | 工程地基：uv/ruff/mypy/pre-commit/CI/src 布局 | 仓库可用 |
| P1 | 内核：配置(JSON5+pydantic+secrets)、structlog、事件总线、启动状态机、supervisor、统一调度器、游戏时钟 | 可启动单进程骨架 |
| P2 | 网络：帧协议(msgpack)、连接管理(验证/心跳/背压)、网关、async RPC、ZMQ 总线、跨服代理 | 双机互通 |
| P3 | 数据：asyncmy 池、redis.asyncio、版本化 schema 迁移、显式声明 ORM、自动存盘、msgpack+schema_version | 持久化闭环 |
| P4 | 热更+防护网、默认安全控制台、watchfiles 监听、Prometheus 指标与事件循环延迟监控 | 可运营 |
| P5 | 业务门面包、模板项目、文档、性能基线 | v1.0 RC |

## 已知问题 -> 根治点（原型 22 项）

| # | 原型问题 | 根治 |
|---|---|---|
| 1 | RPC 回调字典泄漏 | P2：调用表超时清理 |
| 2 | 共享内存单槽覆盖丢数据 | P2：删除共享内存，纯 ZMQ |
| 3 | GetServerList exclude 分支 TypeError | 绿地消灭 |
| 4 | DS 心跳与查询抢连接池、1s 超时误杀 | P3：独立心跳连接、5s×3 告警再杀 |
| 5 | 自动探测本机 IP 当身份，多网卡取错即崩 | P1：显式 advertise_ip/bind_ip |
| 6 | Config 吞 AttributeError 返回 None | P1：pydantic 校验 fail-fast |
| 7 | int.to_bytes() 依赖 3.11+ 默认长度 | 绿地消灭：定长编码工具 |
| 8 | DB 密码/token 明文入 SVN | P1：secrets 只来自环境/本地文件 |
| 9 | schema SQL 拼接 | P3：全参数化 |
| 10 | pickle 跨进程/跨网传输 | P2/P3：msgpack |
| 11 | 控制台 eval/exec 常开 | P4：默认禁用，--unsafe-console 显式开 |
| 12 | launch.py except:pass 静默 | 绿地消灭：fail-fast |
| 13 | 全局 monkeypatch print/settrace/warnings/logging | P1：全部删除 |
| 14 | Timer 与 TimeWheel 并存 | P1：合并为单一调度器 |
| 15 | asyncio_redis 停维护、loop= 参数废弃 | P3：redis.asyncio / asyncmy |
| 16 | frameapi 手写存根与真实 API 脱节 | P5：.pyi 自动生成 |
| 17 | 无 requirements/无文档 | P0/P5 |
| 18 | 无关 PyQt5 demo 误提交 | 绿地消灭 |
| 19 | 裸 except 遍布 | P0：lint 规则禁止（BLE/E722） |
| 20 | 匈牙利命名/Tab 缩进 | 全局：PEP8 + ruff format |
| 21 | 卡顿监控只打印 | P4：指标 + 告警回调 |
| 22 | 存盘无 schema 版本/迁移 | P3：schema_version + 迁移工具 |
