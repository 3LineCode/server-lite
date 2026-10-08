# pyline 迁移收尾方案（P6 · v1.0 RC）

本方案是 `docs/plan.md`（已批准主计划）的收尾执行版，覆盖三件事：

1. **缺陷根治**：对全仓代码评审确认的 30 项缺陷（F-01..F-30）逐条给出按业界/官方
   资料评估后的最优修复方案与验收标准；
2. **功能保真**：旧仓 `F:\ServerLite`（约 7000 行，作为行为规格 oracle）对业务暴露的
   全部功能逐项对照新仓，缺失与细节丢失项全部补齐；
3. **收尾交付**：业务门面包、模板业务公共层、集成/混沌测试、文档，达成 v1.0 RC。

三条总原则（来自迁移要求，冲突时按此排序）：

| # | 原则 | 含义 |
|---|---|---|
| 1 | 旧仓功能及细节保留 | 旧仓业务代码视角的**语义**必须在新仓有等价物（含默认值、编号基数、SQL 形态、日志行为等细节）；有既有持久化数据的语义（日/周/月编号、blob 格式）**必须字节级兼容** |
| 2 | 工程和编码规范按新仓 | async/await 全面化（不做回调兼容层）、mypy `--strict` 零错、ruff 零告警、无模块级单例、无 import 副作用、显式 `Context` 注入、PEP8 命名 |
| 3 | 方案按业界/官方最优 | 每个设计决策给出官方文档或业界共识依据（见各条"依据"与附录 B） |

---

## 1. 基线

### 1.1 已达成（本轮验证过）

- 97 项测试全通过；`ruff check` 与 `mypy --strict`（41 文件）零告警——README 声称的
  工程标准属实。
- 就地热更核心算法、RPC 四路防泄漏清理、慢消费者背压、配置 fail-fast + secrets、
  控制台 eval 三重闸门、消息全 msgpack 化，均为真材实料。
- 旧仓 22 项已知问题中，绿地消灭类（#3/#7/#12/#18/#19/#20）与决策重构类
  （#1/#2/#5/#6/#8/#10/#11/#13/#14/#15）已落实；#4 心跳策略已按"5s×3 告警再杀"重写。

### 1.2 评审确认的缺陷（本方案 F 编号）

分级汇总（全部带 file:line 证据，已在评审报告中核实）：

| 级别 | 数量 | 代表 |
|---|---|---|
| 高（数据丢失/不可部署/静默损坏） | 7 | 存盘 3 次失败永久丢弃、首启建库顺序、>1MB `@rpc` 分块静默损坏、序列化迁移链未接线 |
| 中（可用性/可靠性/安全） | 16 | RPC send 异常残留 pending、ROUTER 头阻塞、热更签名不校验、启动失败被吞 |
| 低（一致性/文档/死代码） | 7 | metrics 未接线、`max_frame_size` 死配置、时钟编号基数不一致（保旧） |

---

## 2. 旧仓 → 新仓功能保真对照

状态图例：✅ 等价已实现 ｜ 🔧 本方案补齐 ｜ 🔄 语义重设计（决策已批准，映射见文） ｜ ❌ 决策不迁移（附录 A 给理由）

### 2.1 进程与服务信息（旧 `aiocom/aioinfo.py`）

| 旧仓 API（位置） | 旧语义要点 | 新仓现状 | 动作 |
|---|---|---|---|
| `GetServiceNo()` (aioinfo.py:162) | 进程索引×100000+服务器号 | `Context.service_no` 同公式 | ✅ + 🔧 facade |
| `GetServiceName()` (:344) | 本服名/指定服名 | `ServerRegistry.entry().name` | ✅ + 🔧 facade |
| `IsMainProcess/IsSubProcess/IsDSProcess/IsGSProcess/IsSingleProcess/IsDevelop` (:119-159) | 带可选服务号判定远端 | `Context` 有自身判定，缺"按号判定" | 🔧 facade 补全（含按号判定重载） |
| `GetMainServiceNo/GetDSServiceNo/GetGSServiceNo` (:166-187) | 号段换算 | `context.py` 有 `db_service_no`，缺通用换算 | 🔧 facade 补 `service_no_of(process_type)` |
| `GetServerList(exclude)` (:326) | 排除分支旧仓必抛 TypeError（问题#3） | `all_servers(exclude=)` 已修 | ✅（#3 根治） |
| `GetServerIP/GetServerPort/GetServerByIP/GetProxyList/IsProxyServer` (:336-362) | 注册表查询 | `ServerRegistry` 均有 | ✅ + 🔧 facade |
| 端口偏移 `>10000→+idx*10000 else +idx*1000` (:275-285) | client/server_port 同规则 | `models.py:114-129` 已保留 | ✅ |
| `GetLocalHost()` 自动探测当身份 (:287) | 多网卡取错即崩（问题#5） | 显式 `advertise_ip` | ❌ 决策#5 |
| `GetSocketHost/GetSocketBindHost/GetSocketConfig/GetServerConfig/GetLogDir` | 配置访问 | `settings` + facade | 🔧 facade |
| `IsWindows/IsLinux/GetMainPid/IsBootWithCode` | 平台/模式 | 前 3 🔧 facade；编译模式 | ❌（无 pyinstaller 路线，附录 A.4） |

### 2.2 任务与生命周期（旧 `taskctrl.py`、`aio_core`、`flowctrl.py`）

| 旧仓 | 旧语义要点 | 新仓 | 动作 |
|---|---|---|---|
| `CreateTask(coro, cb, check_quit)` | 无参回调；异常走全局 RaiseError；`check_quit` 退出期转 QuitTask | `asyncio` 任务由业务自管；退出期收编语义缺失 | 🔧 facade `task.spawn(coro, *, on_quit=False)`：`on_quit=True` 且关停中→记入退出等待集（修复 F-19 时实现） |
| `CreateStartTask` | 阻塞启动状态机推进 | `lifecycle.track_start_task` | ✅ + 🔧 facade；失败即中止启动（F-19，不再吞） |
| `AddStartWait(flag)` | 重复 flag 抛错；返回释放回调 | `lifecycle.add_start_wait` | ✅ + 🔧 facade |
| 退出任务清单每 10s 打印 (taskctrl.py:94-104) | 关停可观测 | 无 | 🔧 supervisor 关停日志列出未退出任务（带 `coro` 路径） |
| `StopAio(reason)` | 反向 FuncQuit → 等退出 → KillProcess | `request_shutdown` + teardown | ✅（F-19/F-20 修复启动/关停互斥与信号） |
| `KillProcess(reason)` | 立即自杀 | console kill 已有同语义 | ✅ + 🔧 facade `env.kill(reason)` |
| 9 态启动状态机 (flowctrl.py:10-19) | DS 跳过 Base/FuncInit；CanUseDB 门槛；OpenLogin 仅主进程 | 10 态枚举 + 转移表；DB 进程分发差异保留 | ✅（逐态对照已核对） |

### 2.3 定时器（旧 `timer.py`）

| 旧仓 | 语义要点 | 新仓 | 动作 |
|---|---|---|---|
| `NewTimer().Call/MsCall(flag, delay, func, *a)` | flag 键控；同 flag 重设=取消旧排新；delay 下限 0.001s；协程/普通函数双支持 | `Scheduler` 内核有短/长路径，无业务 flag API | 🔧 `api.timer`：`TimerFacade`（内部 flag→entry 字典，语义逐条保留，长延迟自动走 wheel 路径） |
| `Left(flag)/Del(flag)/SoonCall` | 剩余秒（无任务=0）；删并取消；call_soon | 无 | 🔧 同上 |
| `TimeWheel` (:98-154) | 预留未用 | 已并入 Scheduler（决策#14） | ✅ |

### 2.4 事件系统（旧 `aioevents/pubevents/events` + `aioonly`）

| 旧仓 | 新仓 | 动作 |
|---|---|---|
| 三层正/逆序派发（CallEvent/ReverseCallEvent） | `EventBus` LAYER_FRAMEWORK/PUBLIC/BUSINESS + quit 反向 | ✅ |
| `OnEnvInit`（loop 启动前） | 无对应（新仓最早是 FrameInitEvent） | 🔧 新增 `EnvReadyEvent`：配置/Context 构建完成、boot 步骤开始前派发（async 语境下的等价挂点） |
| OnFrameInit/OnBaseInit/OnFuncInit/OnFuncDone/FuncQuit | FrameInit/BaseInit/FuncInit/FuncDone/FuncQuit Event | ✅ |
| HalfHour/NewHour/NewDay + 隐式 NewWeek/Month/Year | 全部已成正式事件类 | ✅（新仓更完整） |
| `Connected(oLinker)`（载荷=连接对象） | `ClientConnectedEvent(peer=(ip,port))` 载荷只有元组 | 🔧 载荷补 `connection` 句柄（连接对象含 send/close 能力，对齐旧 Linker 能力面） |
| `InputCmd(sCmd)`（`$`/`￥` 前缀） | `ConsoleCommandEvent` | ✅ + 🔧 补 `￥` 前缀 |
| PreReload/OnReload | PreReload/OnReloadEvent | ✅ |
| `CallEvent` 返回值 or-链（第一个真值短路） | 广播语义 | ❌ 不迁移（async 下请求-响应走 RPC；附录 A.6） |

### 2.5 RPC（旧 `rpccall.py`）

| 旧仓 | 语义要点 | 新仓 | 动作 |
|---|---|---|---|
| `RpcCall/RCB/RpcFunc` | 回调式，`RpcFunctor(cb, ovt, err, timeout=10)` | `await rpc.call(srv, path, *args, timeout=)` + future | 🔄 决策#3：`ovtfunc`→`except RpcTimeoutError`、`errfunc`→`except RpcError`，语义完备 |
| 元组结果拆参回调（R_TUPLE） | `cbfunc(*result)` | `await` 直接返回 tuple | ✅ 等价 |
| 无回调 fire-and-forget | needBack=0 | `rpc.notify(...)` | ✅ |
| `RpcFromServer()` | 被调方取调用方服务号（contextvar，调用结束归 0） | **缺失**（`_execute` 持有 `from_service` 但未暴露） | 🔧 `rpc.current_caller() -> int`（`contextvars.ContextVar`，0=无调用上下文） |
| 调用表泄漏（问题#1） | 旧仓超时不删表项 | 四路清理 | ✅（F-14 再补 send 失败路径） |
| 超时不中断远端执行 | 旧新相同 | 超时不发 CANCEL | 🔧 F-18：超时/调用方取消时向远端发 MSG_CANCEL（`_on_cancel` 已有处理端，补发送端） |
| 函数不存在 KeyError | 报错语义 | `RpcError("unknown function")` | ✅ |
| 子进程禁止直发跨服 (transfer.py:46-49) | 路由层硬校验 | `router.py` CrossServerError | ✅ |

### 2.6 协议与网关（旧 `package.py`、`base/network.py`、`gateway.py`）

| 旧仓 | 语义要点 | 新仓 | 动作 |
|---|---|---|---|
| 子协议号 handler 注册/重复检测/未注册告警丢弃计数 | `Gateway.RegisterNetwork` | gateway/router 有；未注册路径 🔧 补"告警+计数丢弃"（主计划原文承诺，未落地） |
| `PreparePacket/PacketInt/...` 手工打包 API | 定长 int + msgpack 字段 | 结构化 msgpack 消息 + dataclass codec | 🔄 决策#10：不保留字节级手工打包；提供类型化消息注册（见 4.2）；差分测试做**语义**差分而非字节差分（附录 A.7） |
| `Network.__reloadkeep__=("m_RefInstance",)` + 热更自动 `ReInitHandlers` | 活实例引用保留、热更后重注册 handler | 无 | 🔧 F-30：协议注册表支持 `__reload__` 钩子（热更后对活实例重跑 handler 绑定） |
| 大字段 `PacketData` 前还原 Obs 容器（RTransObs） | 序列化前解包 | TrackableModel 显式 `get_data()` | ✅（Tracked 容器见 4.3） |

### 2.7 连接管理（旧 `linker.py`、`client/`）

| 旧仓 | 语义要点 | 新仓 | 动作 |
|---|---|---|---|
| 首帧 token 验证 + 5s 超时 + verify/client 日志 | 无加密无挑战 | 握手帧带 token + deadline | ✅（verify 成功/失败日志 🔧 核对补齐 verify.log/client.log 等价物） |
| `AddCloseHook`（已关闭则立即执行）/`AddVerifyHook` | 钩子语义 | close hook 有；verify hook 🔧 | 🔧 对齐 |
| `DisConnect(reason)` | 触发 close hooks + OnDisConnected | `connection.close(reason)` | ✅ |
| 客户端仅主进程、OPENLOGIN 后开放登录 | flowctrl:104-109 | runtime 启动完成才 listen | ✅ 核对通过 |
| 断线检测（ConnectionError/IncompleteRead → break → DisConnect） | 读循环退出即清理 | 同 | ✅ |
| 服务端 5s 验证超时、空闲检测 | — | idle_timeout 有；**服务端从不主动 ping** | 🔧 F-17：服务端侧空闲探测（对等 `@ping`），第三方客户端不再被误杀 |
| 背压/慢消费者 | 无（旧仓缺失） | 有界队列 + 溢出断连 | ✅（新仓增强） |

### 2.8 MySQL / ORM / 存盘（旧 `dbmysql/`）

| 旧仓 | 语义要点 | 新仓 | 动作 |
|---|---|---|---|
| aiomysql 池 + `select 1` 1s 心跳失败 KillProcess | 问题#4 | asyncmy + keepalive 5s×3 | ✅（#4 根治）+ F-10 专用连接 |
| `SET SESSION TRANSACTION ISOLATION LEVEL {level}` 逐连接一次 | level 无校验 | 同样无校验 | 🔧 F-09：`Literal["READ UNCOMMITTED","READ COMMITTED","REPEATABLE READ","SERIALIZABLE"]` 白名单（pydantic 官方推荐用法），从根上消灭 SQL 注入向量 |
| `MysqlExecute/MysqlQuery`（不可直连进程自动代理 DS；启动期挂 AddStartWait） | 无 DB 权限时静默 `func()`/`func(())` | `DatabaseAccess` 统一 `await`（本地直连/远程 RPC 自动切换） | 🔄 静默成功→显式 `ConnectionError`：旧语义掩盖配置错误，违背新仓 fail-fast（附录 A.8）。启动期阻塞语义由 boot 门保证 |
| 缺表则建（递归对账） | 无列级迁移无版本（问题#22） | `SchemaManager.ensure_all` | 🔧 F-05/F-06 重构为版本表迁移（见 3.2） |
| `Table/Column/PrimaryCol` 声明：bUnique/default/bNull/长度/注释 | DDL 生成 | `TableFieldDef` 只有 type/primary/comment | 🔧 F-09：补 `unique/default/null` 字段（默认值走参数化/字面量白名单，注释转义） |
| 列存 SQL：`INSERT ... AS new ON DUPLICATE KEY UPDATE col=new.col` | 单列 UPSERT、值参数化 | `schema.py:103` 同形态 | ✅（字节级兼容保留） |
| `ColumnSave/RowSave`：构造即自动异步加载；`MakeSave/LoadSave/InitSaver/OnLoadDone` 契约；`m_NoNeedSave` | saver 生命周期 | `DataSaver`/`TrackableModel` 显式字段（决策：替代 co_names 魔法） | ✅ 契约对齐 + 🔧 补 `no_save` 开关、`add_load_hook`、`flush_now`、`is_loaded/is_deleted` 便捷名（facade 层按旧名导出别名） |
| 属性变更自动标脏（ObsData/ObsDict/ObsList） | 容器原地改也触发 | `touch()` 显式；容器原地改**不**标脏 | 🔧 4.3 `TrackedDict/TrackedList`（构造时绑定 owner，变更自动 touch；显式声明替代字节码魔法，语义等价） |
| `SaveCtrl`：5s 一轮、每轮 50、FIFO；关停 QuitSaveAll 全量+禁入队 | 失败不重试直接丢 | `SaveScheduler` 同节奏 | 🔧 F-01/F-02 重构失败策略（3.1，核心） |
| `AtOnceSave(bForce)/CreateData/DeleteData/IsLoaded/IsDelete/AddLoadHook` | 便捷 API | 部分 | 🔧 facade 补齐（含 `force` 语义） |
| `LoadFromMysql` 空结果→`LoadSave(None)` 新数据标记 | 无记录≠错误 | 同 | ✅ |
| 删除=先置位再 DELETE | SQL 失败状态卡死 | 同问题 | 🔧 F-04 |
| 并发构造同 key saver | 竞态回写 | 竞态仍在 | 🔧 F-03 |
| `MYSQL_INT/STR/TEXT/DATA` 常量 | BIGINT/VARCHAR/MEDIUMTEXT/MEDIUMBLOB | `schema.py` 内部有 | 🔧 facade 导出（业务建表声明用） |

### 2.9 Redis（旧 `dbredis/`）

| 旧仓 | 新仓 | 动作 |
|---|---|---|
| `RedisSet/Get/Del/MultiDel`（Get 空→None；无 DB 时炸） | `DatabaseAccess` 有 set/get/del，**缺 multi_del** | 🔧 补 `RPC_REDIS_DEL_MANY` + `delete_many`；Get None 语义已对齐 |
| asyncio_redis（停维护，问题#15） | redis.asyncio | ✅ + 🔧 F-11 官方推荐参数：`socket_timeout`/`socket_connect_timeout`/`health_check_interval`（redis-py 文档），去掉死分支 |

### 2.10 序列化与数据迁移

| 旧仓 | 新仓 | 动作 |
|---|---|---|
| ORM blob = pickle | msgpack + `PLD1|version` 头 | ✅（#10/#22） |
| —（无版本机制，问题#22） | `loads_migrated` 存在但 **ORM 解码路径未接线** | 🔧 F-07：解码路径 `peek_version → loads_migrated`；遇到**更新**的版本号（旧代码读新数据）显式 `BlobVersionError` fail-fast；`from_dict` 未知键过滤保留为前向容错 |
| — | — | 🔧 F-08：一次性迁移 CLI：读旧库 pickle blob → 转新格式回写（主计划第五节承诺，落地为 `python -m pyline.tools.migrate_pickle`，dry-run 默认） |

### 2.11 热更（旧 `reload.py`）

| 旧仓 | 语义要点 | 新仓 | 动作 |
|---|---|---|---|
| 就地更新四规则（类/property/函数/方法）+ 闭包 cell 递归 + bases 拦截 + 模块 dict 回滚 | 对象地址不变 | `inplace.py` 等价实现（测试证实外部引用/实例跟随） | ✅ |
| `__reload__`（模块函数+类方法，`__module__` 归属校验） | 热更后回调 | 有 | ✅ 核对属性级 `__reloadkeep__` 标记（值自带标记也保留）——旧仓两形态 (reload.py:136-146)，新仓只查类名单 → 🔧 补 |
| `PreReload/OnReload` 事件、嵌套防重入、失败自动回滚 | — | 有 | ✅ |
| 禁改清单仅注释（继承/super/闭包/元类/__slots__/C 扩展） | — | 沙箱结构校验有，但**签名/dunder 不查、沙箱有副作用、TOCTOU、类回滚半截** | 🔧 F-29 四重加固（3.5，含禁忌文档 docs/hot-reload.rst） |

### 2.12 控制台 / 文件监听 / 时钟（旧 `ioevent.py`）

| 旧仓 | 新仓 | 动作 |
|---|---|---|
| 命令：exit/quit/q、kill/k/stop、clear/cls、`$cmd`/`￥cmd`、`update mod,...`、eval/exec | console.py 全有（除 `￥`） | ✅ + 🔧 `￥` 前缀；eval 默认禁用保持 |
| **终端输入仅主进程**（ioevent.py:27-29） | develop 下**每进程**都起 console（含 db 进程） | 🔧 F-27：改回仅主进程（多进程抢同一 stdin 本身就是错的；子进程要 console 走 `--console-process` 显式指定） |
| 时钟事件前写 `clock.log` | runtime 时钟事件无落盘 | 🔧 每次时钟事件追加 `{log_dir}/clock.log`（细节保留） |
| watchgod 200ms debounce、监听 cwd、忽略日志目录与 .vscode | watchfiles 有 debounce | ✅ 🔧 核对忽略目录包含 log_dir |
| 卡顿监控仅打印（问题#21） | LoopLatencyMonitor 指标 | ✅ + 🔧 F-28 告警回调挂钩（阈值可配） |

### 2.13 日志 / 调试（旧 `debugs.py`、`logger.py`）

| 旧仓 | 新仓 | 动作 |
|---|---|---|
| `LogFile/LogDebug`：`{log_dir}/{进程类型}/{file}.log`、`debug/` 子目录 | `log.file_logger`（有模块级缓存 bug） | ✅ 语义 + 🔧 F-22 缓存改入 Context |
| print 劫持/彩色/SetPrintHook/os.log 轮转（7 天×5 个） | ❌ 决策#13；structlog console + `retention_days`/`rotation_mb` | 🔄 语义等价（轮转保留策略），实现走标准 logging.handlers.RotatingFileHandler/TimedRotatingFileHandler（官方标准） |
| `RaiseError` 打印最深帧局部变量/实例属性 | 无（只堆栈） | 🔧 `devtools.exc_format.py`：`format_exception_with_locals(exc)`（traceback 官方 API 遍历帧 + 局部变量渲染，**不劫持任何全局钩子**），接入 console 异常输出与可选日志 handler |
| `TraceMsg`（调用栈+局部变量） | 无 | 🔧 `api.debug.trace(msg)`（同上实现） |
| warnings 劫持→warnings.log | 无 | 🔄 `logging.captureWarnings(True)`（标准库官方机制）+ warnings file handler |
| `sys.excepthook/settrace` 劫持、自研 coverage trace | — | ❌ 决策#13；覆盖率改 coverage.py 官方 API（4.5） |

### 2.14 业务公共层（旧 `public/pubcom`）→ 必须移植进 template

| 旧仓 | 语义要点（持久化兼容！） | 动作 |
|---|---|---|
| `com_time` 全套（GetTime 调试偏移/SetTime/PushTime/TimeString/GetDayNo/GetWeekNo/GetMonthNo/GetWeekDay(iWeekStart=1)/MakeTime...） | **GetDayNo/GetWeekNo 1 基、GetMonthNo 0 基（2024-01=0）**——编号会持久化进 DataOP 容器，**基数不可变**；STANDARD_TIME=(2024,1,1) 锚点 | 🔧 4.4 移植到 `template/game/com_time.py`（薄封装 pyline GameClock + 纯函数），逐函数语义对齐；`SetTime/PushTime` 走 GameClock offset（新仓已有） |
| `TimeData/DayData/WeekData`（d_Time/d_Data/d_Last 三件套、TryNewTime 翻页） | 上周期数据查询（LastGet/LastAdd...） | 🔧 `template/game/containers.py` |
| `DataOP`（Day/Week/Time 限时/永久/Temp 五组 KV + 只留一层历史） | 限时数据惰性过期清理；`LastDayGet` 昨日值 | 🔧 同上；**已知 bug 修正**：`TimeUpset` 的 `time in self.d_Time` 键时长混用判定修为按 key 判定（bug 非功能，附录 A.9） |
| `ColumnSaveOP/RowSaveOP`（DataOP×存盘基类组合） | MRO 语义 | 🔧 `TrackedModel + DataOP` mixin 等价组合 |
| `pubevents` 三层骨架 | 空实现占位 | ✅ template 已有 events 骨架 |

### 2.15 配置（旧 `aioconfig/*.json`）

| 旧仓 | 新仓 | 动作 |
|---|---|---|
| project.json/server.json/tables.json + 行注释 | JSON5 + pydantic（#6）+ base 单层继承 | ✅ |
| 数字键 `"1"`/`"0001"` 碰撞静默覆盖 | 同样存在 | 🔧 F-26 加重复号检测 |
| shrmem.max_mem（eval 字符串） | 共享内存已删（决策#2） | ❌ |
| token 单一（客户端/内部共用） | 同样单一 | 🔧 F-16：拆 `socket.token`（客户端）与 `socket.inter_token`（服务器间/proxy，缺省回落 token 以兼容旧部署） |
| mysql.pswd/redis.pswd 明文 | secrets 机制 | ✅（#8）+ 🔧 F-26 `$plain:` 空值语义修正（models.py:64 文档与 secrets.py:36 实现矛盾） |

### 2.16 启动流程与进程模型

| 旧仓 | 新仓 | 动作 |
|---|---|---|
| 多进程拉起 + 子进程 1s 监视父进程存活（死亡即退） | supervisor 双向监视 + 宽限 kill | ✅（F-21 补异步 join 与测试） |
| 主进程 Ctrl+C → KeyboardInterrupt 粗暴退出 | **同样没装信号处理器** | 🔧 F-20（3.4） |
| pyfiglet banner + 源码/编译模式打印 | 无 | 🔧 简化 banner：structlog 多行输出项目名/服务名/服务号/绑定地址/Python 版本（**不引入 pyfiglet**——纯装饰依赖；`FrameShowFinish` 的"Server -> / Client ->"监听地址打印细节保留） |
| win32 Proactor / Linux uvloop | `net/loop_policy.py` | ✅ |
| `multiprocessing.freeze_support`/pyinstaller 打包脚本 packet.py | — | ❌ 决策#8（uv 目录分发） |

---

## 3. 缺陷修复设计（按最优方案）

### 3.1 M1 数据安全（最高优先，先行合入）

#### F-01 存盘失败策略：永不丢脏数据

现状：`autosave.py:85-95` 失败 3 次从队列永久移除，关停 `flush_all` 也不补救；`retry_cooldown` 写了但从未生效（`_deferred` 不 gate `_dirty`）。

**方案**（游戏服务器行业共识：脏数据生命周期 = 直到落盘成功或进程死亡，中间只有退避没有丢弃）：

1. 删除 `max_attempts` 丢弃路径。失败 saver 留在脏集，按**指数退避**重试：
   `next_retry = now + min(cooldown * 2**(failures-1), 300s)`（上限 5 分钟）；
2. 重试节奏由 `_deferred` 时间戳驱动（修复 cooldown 失效：`flush_batch` 跳过未到期的 saver）；
3. 连续失败达到阈值（默认 3 次）→ 触发**告警回调**（F-28 的 alarm hook，接 Prometheus `save_failures_total` + 日志 CRITICAL），不丢数据；
4. 队列深度暴露 `save_queue_depth` gauge；深度超阈值同样告警（背压可观测）；
5. 关停语义：`flush_all` 改为"带总时限的循环重试"（默认 60s，可配）：时限内未清空 → CRITICAL 日志逐 saver 列出 key + 拒绝正常退出码（非零退出，让 supervisor/运维感知数据未落盘）。

**依据**：持久化队列标准做法（无限重试 + 退避 + 死信告警，参照 Redis/NSQ 等持久队列的 at-least-once 语义）；"关停冲刷有界重试"参照 PostgreSQL shutdown checkpoint 思想。

**验收**：`test_autosave_retry_keeps_dirty`（失败 N 次后数据仍在队列且退避递增）；`test_autosave_shutdown_flush_retries`（DB 恢复后关停冲刷成功）；`test_autosave_shutdown_deadline_reports_loss`（超时非零退出 + 逐 saver 日志）。

#### F-02 关停取消窗口丢 saver

现状：`stop()` cancel 任务时，`flush_batch` 可能已 pop 未 flush（autosave.py:73-74）。

**方案**：改"pop→in-flight 集合→flush→从 in-flight 移除"；`stop()` 先停调度、await 在途 flush（`asyncio.gather` in-flight，带超时），`flush_all` 遍历 `_dirty ∪ in-flight`。这同时消灭"pop 后取消丢一条"窗口。

**验收**：`test_autosave_stop_drains_inflight`。

#### F-03 并发 `load()` 竞态

现状：第二个调用者在 LOADING 中拿到 `None`（orm.py:140-141），误判无数据后 `set_data` 覆盖、随后被首个查询结果回滚覆盖。

**方案**：in-flight future 去重（`asyncio` 官方推荐的 single-flight 模式）：LOADING 中返回同一个 `Future`，首个 load 完成时 `set_result`。`load()` 变为 `async def` 返回数据（业务层 `await saver.load()`），构造即自动加载的旧语义用后台 task + `add_load_hook` 保留。

**验收**：`test_orm_concurrent_load_singleflight`（并发 50 个 load 只发 1 条 SQL，结果一致）。

#### F-04 `delete()` 状态机

现状：先置 `DELETED` 再 DELETE（orm.py:188-190），SQL 失败状态卡死。

**方案**：先执行 DELETE，成功后再置 `DELETED`；失败抛出且状态不变（可重试）。删除中的重复调用幂等（返回同一进行中的 future）。

**验收**：`test_orm_delete_failure_keeps_state` / `test_orm_delete_idempotent`。

#### F-05 首启建库顺序

现状：先 `connect(db_name)` 后 `CREATE DATABASE`（runtime.py:164-167），全新 MySQL 必失败。

**方案**（MySQL 官方运维惯例）：`ensure_database()` 先用**无库连接**（不指定 db / 连 information_schema）执行 `CREATE DATABASE IF NOT EXISTS ... CHARACTER SET utf8mb4`，再建池；随后 `ensure_all()`。迁移到 SchemaManager 内，`runtime._connect_local_db` 只调用 `schema.ensure()`。

**验收**：`test_schema_creates_database_first`（用 CI 的 MySQL 容器，标记 `@pytest.mark.mysql`）。

#### F-06 版本化 schema 迁移

现状："版本化"无版本表；列已存在但定义漂移不检测（schema.py:170-172 仅按名比对）。

**方案**（Alembic `alembic_version` 单行表模式 + MySQL 8.0 Online DDL 最佳实践）：

1. 建单行表 `` `pyline_schema` (table_name VARCHAR(64) PK, version INT NOT NULL) ``；
2. `ensure_all()` 流程：缺表 → 建表并记 version；已有表 → 比对 `information_schema.COLUMNS` 的**声明漂移**（类型/长度/NOT NULL），漂移即启动失败并输出 diff（**只报错不自动 ALTER**——线上 DDL 必须显式）；
3. 迁移脚本机制：`migrations/` 目录按 `NNN_<name>.sql` 编号，启动时按版本表顺序执行**未应用**的脚本，事务包裹 + 版本表同事务提交（MySQL DDL 不回滚，因此脚本必须**幂等**且**只加不改**——expand-contract 模式：expand 加列/表/索引，contract（删旧列）留给人工窗口）；
4. 生成的 ALTER 一律显式 `ALGORITHM=INSTANT, LOCK=NONE`（MySQL 8.0 官方 Online DDL）：不满足 INSTANT 条件时启动即报错，绝不静默回退成 COPY 重建大表。

**依据**：Alembic 版本表模式；MySQL 8.0 Online DDL 官方文档（INSTANT 限追加列、元数据级变更）；expand-contract（parallel change）业界共识。

**验收**：`test_schema_version_table`、`test_schema_drift_detected`、`test_schema_migration_linear_apply`（mysql 标记）。

#### F-07 迁移链接线 + F-08 旧数据迁移工具

见 2.10。`MsgpackCodec.decode` 改为：`peek_version(blob)` → 当前版本直接 `loads`；旧版本走 `loads_migrated`；**新版本**（数据比代码新）抛 `BlobVersionError`（fail-fast，错误信息含 blob 与代码版本号）。

迁移工具 `pyline.tools.migrate_pickle`：按 tables.json 逐表扫描 blob 列，`pickle.loads`（受限于 `restricted pickle`——只允许 dict/list/tuple/int/float/str/bytes/None/bool，官方 `pickle` 安全建议）→ `dumps`（当前版本）→ 回写；`--dry-run` 默认，`--execute` 生效，输出统计与失败明细。

**验收**：`test_migrate_pickle_tool`（临时库往返）；`test_decode_rejects_newer_version`。

#### F-09 SQL/DDL 加固

- `isolation_level`：`Literal[...]` 白名单（models.py），杜绝 f-string 注入向量；
- 表/列 COMMENT 一律经 `escape`（`'`/`\` 转义，schema.py:97 现状只转义列注释）；
- `TableFieldDef` 补 `unique: bool=False`、`default: str|None`（白名单字面量：数字/带引号字符串经转义）、`not_null: bool`；旧 DDL 规则保留：TEXT/BLOB 列不写 NOT NULL（dbsetup 语义）。

#### F-10 MySQL keepalive 专用连接

现状：keepalive 从共享池 acquire（与业务抢连接，问题#4 只修了一半），异常抛在无人 await 的 task（mysql.py:102-113）；`_configured_conns` 按 `id(conn)` 永不清理。

**方案**：keepalive 用**池外独立连接**（创建池时同步开一条专用 keepalive 连接，`SELECT 1` 每 `keepalive_interval`，连续 `miss_limit` 次失败：告警回调 → 再失败关停）；任务异常经 done-callback 记录（不再无人 await）；`_configured_conns` 改 `weakref.WeakSet`（官方推荐，id 复用误跳一并消灭）。

#### F-11 Redis 客户端健壮性

redis-py 官方参数：`socket_timeout`、`socket_connect_timeout`、`health_check_interval=30`、`retry_on_timeout`；去掉 `decode_responses=True` 下的死分支（redis.py:53-54）。连接失败重连由 redis-py 内建（连接池自动重连）。

---

### 3.2 M2 网络健壮性

#### F-12 分块重组统一（修静默损坏）

现状：编码端对**所有** flag 分块（protocol.py:54-62），解码端 `@` 前缀帧绕过重组（protocol.py:93-95）→ >1MB 的 `@rpc`/`@fwd` 静默碎掉，表现为假超时。

**方案**：分块重组是**传输层职责，与 flag 语义正交**——删除 `@` 豁免，所有 flag 一致走 `_accumulate_chunk` 重组后才上抛。配套：

- `Connection` 构造传入 `SocketSettings.max_frame_size`（现状 connection.py:70 不传，配置无效）；
- 单条逻辑消息总重组上限 = `max_frame_size`（超限即断连，现状已有逻辑保留）；
- flag 段非 UTF-8 → `ProtocolError`（而非 UnicodeDecodeError 穿透，protocol.py:130）；
- 交错分片报错信息带上期望/实际 flag；
- 新增校验：**重组后单消息大小**也受 max_frame 约束（chunk 头部声明的总长先行校验，内存不放行超限重组）。

**依据**：长度前缀分帧的业界标准做法（HTTP/2 DATA 帧、Kafka record batch：分帧/重组完全由传输层负责，与应用消息类型无关）。

**验收**：`test_protocol_at_flag_chunk_roundtrip`（>1MB 的 `@rpc` 消息编码→解码得到原 payload）；`test_connection_max_frame_wired`。

#### F-13 单帧不可拆连接（分发隔离）

现状：`connection.py:138` 裸调 `_on_message`；RPC 解包无字段数校验（rpc.py:199/237/254）——一条畸形 `@rpc` 帧的 ValueError 穿到读循环 except，整条连接被当 EOF 拆除；proxy 链路同理两机断链。

**方案**（错误隔离边界原则：传输层异常才动连接，应用层异常只丢消息）：

1. 读循环内分发包裹 `try/except Exception`：记日志 + `dispatch_errors_total` 计数 + **丢弃该帧继续读**（仅 `ProtocolError`/EOF/连接层异常才关闭连接）；
2. RPC 每类消息先验 `isinstance(message, list) and len(message) == N`（call=5 / result=4 / cancel=3），不合法 → `ProtocolViolation` 日志 + 计数 + 丢弃；
3. 畸形帧速率超阈值（默认 10/s）→ 主动断连（防打日志 DoS）；
4. ZMQ 路径已有同类兜底（ipc.py:130-133），统一抽出 `safe_dispatch()` 帮助函数，TCP/ZMQ/proxy 三路一致。

**验收**：`test_connection_malformed_rpc_frame_keeps_connection`（发 3 条畸形帧后正常消息仍通）；`test_rpc_message_arity_validation`；`test_malformed_rate_limit_disconnects`。

#### F-14 RPC 清理与错误传播

现状：`_send` 在 try 外（rpc.py:139-142），路由异常时 pending/timer 残留；结果不可序列化只打日志（rpc.py:261-267）→ 调用方假超时。

**方案**：

1. 先注册 pending（含 timer），再 `try: send... except: future.set_exception(exc); cleanup()`；
2. `_execute` 成功后 `packb(result)` 失败 → 回发 `[MSG_RESULT, call_id, 0, repr-错误摘要]`（复用错误通道，调用方得到 `RpcError("result not serializable: ...")` 而非超时）；同理参数打包失败在调用方立即抛 `TypeError`；
3. timeout/cancel 路径已有，补 F-18（下条）。

**验收**：`test_rpc_send_failure_cleans_pending`（断言 `pending_count()==0` 且 future 立即完成）；`test_rpc_unserializable_result_raises_rpcerror`。

#### F-18 RPC 超时/取消传播到远端

超时或调用方取消时补发 `MSG_CANCEL`（对端 `_on_cancel` 已存在，rpc.py:253-257，只缺发送端）。对端无响应不重试不等待（best-effort 中断）。可选配置 `cancel_propagation=True`。

#### F-15 ZMQ 总线：消灭头阻塞与发送风暴

现状：ROUTER 转发内联在收包循环（ipc.py:130-150）——目标 DEALER 慢导致 HWM 满 → `send_multipart` 等 POLLOUT → **全总线停摆**；每条消息 `create_task` 发送（:91）——无背压且理论上可乱序。

**方案**（ZeroMQ 官方 zguide 第 4 章队列代理模式 + zmq_socket(3) 语义）：

1. **单写者模型**：每个 socket 一个常驻 sender task，从 `asyncio.Queue`（有界，= HWM 相关配置）取消息串行 `send_multipart`——串行化天然保序，有界队列天然背压（满时对 ROUTER 侧按对端暂停取包）；
2. ROUTER 收包循环只做：recv → 查路由 → 投递到目标对端的**每对端队列**（`dict[peer_id, Queue]`，慢对端队列满只阻塞该对端的投递 task）→ 立即继续 recv；
3. `ZMQ_ROUTER_MANDATORY` 保留：未知 peer 抛错计数（现状正确）；HWM 显式配置已有，补 `unroutable_sends`/每对端队列深度指标；
4. DEALER 侧同样单写者队列。

**依据**：zguide Ch.4（ROUTER 永不阻塞、慢消费者隔离、Majordomo/队列代理的 per-worker 管道）；libzmq 文档（HWM 满时 ROUTER 默认静默丢包，MANDATORY 模式报 EHOSTUNREACH）。

**验收**：`test_ipc_slow_dealer_does_not_block_bus`（一个慢 DEALER 时其余进程消息延迟 <100ms）；`test_ipc_send_order_preserved`；`test_ipc_backpressure_bounds_memory`。

#### F-16 跨服 proxy：重连健壮 + 身份核验 + 双 token

现状：`_maintain` 里 IDENT 发送在 try 外（proxy.py:179），连接刚建即关的窗口下重连任务永久死亡；IDENT 无核验可被同 token 机器冒名（proxy.py:82-90）；客户端与内部 RPC 共用 token 与 gateway（runtime.py:238-247）。

**方案**：

1. `_maintain` 整个循环体纳入 try/except + 指数退避重连（对齐 ZeroMQ 内建重连语义）；
2. IDENT 握手升级挑战式：proxy 服务端回 `IDENT_OK(machine)` 前核验（a）源 IP 在注册表 `advertise_ip` 中且与声明 machine 号一致（现状有 IP 白名单，补号-IP 一致性）；（b）`inter_token`；（c）**machine 号重复声明拒绝**（防劫持：已有同号连接时新连接拒绝并 CRITICAL 告警）；
3. 配置拆 `socket.token`（客户端）与 `socket.inter_token`（内部互联，缺省回落 token 兼容旧部署）；gateway 分发按来源标记 `internal/external`，**客户端来源的 `@rpc`/`@fwd`/`@bye` 之外一律拒绝**（客户端网络不能直调内部 RPC 面）。

**验收**：`test_proxy_reconnect_survives_immediate_close`；`test_proxy_ident_machine_conflict_rejected`；`test_client_cannot_invoke_internal_rpc`。

#### F-17 连接生命周期补全

1. `close()`：`@bye` 发送后 drain 有界等待（复用发送队列冲刷，`wait_closed` 收尾）——优雅关闭不丢在途消息；
2. 服务端空闲探测：`idle_timeout` 到期前服务端主动发 `@ping`，客户端必须回 `@pong`（旧客户端语义：旧仓客户端主动 ping、服务端被动；新仓对等探测，两者兼容：收到对端 ping 回 pong 即刷新活性）；
3. 服务端握手确认：验证成功后服务端回 `@welcome`，客户端 `verified` 以此为准（现状 connection.py:85-86 无条件置位，token 错误的客户端自认为已验证）。

**验收**：`test_close_flushes_pending_writes`；`test_idle_server_ping`；`test_client_verified_requires_welcome`。

---

### 3.3 M3 生命周期 / 调度 / 内核

#### F-19 启动失败不再被吞 + 关停/启动互斥（评审最高危）

现状：`_start_task_done` 只打日志（lifecycle.py:156-161，注释与实现矛盾）；`request_shutdown` 置 QUIT 后 `run_boot` 门循环永不退出（lifecycle.py:129-133 只查 `stuck_error`）→ 关停发生在启动期 = 进程挂死。

**方案**：

1. done-callback 捕获启动任务异常 → 存入 `self.stuck_error`（复用现有通道）→ `run_boot` 门循环检测到即抛出，启动失败 fail-fast；
2. `run_boot` 门循环与 watchdog 循环条件统一改为同时检查 `QUIT`：关停请求到达即取消剩余启动任务、按已启动组件执行 teardown、退出 `run_boot`（不抛 stuck 错误）；
3. `request_shutdown` 幂等保护保留；二次信号（SIGINT×2）升级为立即 kill（业界惯例：一次优雅、两次强杀）。

**验收**：`test_boot_fails_when_start_task_fails`；`test_shutdown_during_boot_exits_cleanly`；`test_double_signal_forces_kill`。

#### F-20 主进程信号处理 + teardown 原子链

现状：`main_proc` 不装 handler（runtime.py:445-458），Ctrl+C 跳过子进程宽限关停；`_shutdown_teardown` 无 try/finally 串联（:313-335）。

**方案**（Python 官方 `loop.add_signal_handler` + TaskGroup 语义）：

1. 信号统一 `loop.add_signal_handler(SIGINT/SIGTERM, ...)`（Windows Proactor 下 SIGTERM 不可用时退回 `signal.signal`，官方文档明确的平台差异处理）；
2. 主进程收到信号 → `request_shutdown` → supervisor `terminate_children`（宽限 10s → kill）→ 自身 teardown；
3. teardown 每步独立 try/except + finally 链：任一步失败记日志继续后续步骤（保证 mysql.close 总会执行），全链总时限可配（默认 90s）超时强制退出。

**验收**：`test_main_proc_signal_graceful_shutdown`（集成）；`test_teardown_continues_after_step_failure`。

#### F-21 supervisor 异步化 + 补测试

现状：`child.join(timeout=2)` 同步阻塞 loop（supervisor.py:99-100）；匿名 task 泄漏（:161-163）；**零测试**。

**方案**：join 改 `await asyncio.to_thread(child.join, timeout)`（官方 API）或轮询 `child.is_alive()` + `await asyncio.sleep(0.05)`（更简单，选后者——无线程开销）；所有 task 持强引用并在 stop 统一 cancel+await。补测试套件（spawn/正常退出/子进程崩溃→重启策略（默认不重启，告警）/terminate 宽限/父进程消失检测/Windows+Linux 双路径 mock）。

#### F-22 log 模块级缓存（违反复审第一条军规）

现状：`log/__init__.py:66` 模块级 `_file_channels` 缓存忽略参数变化（同 name 不同 run_dir 写错文件）。

**方案**：channel 注册表移入 `Context.logging`（显式持有，随 Context 生命周期销毁）；模块内只留纯函数 + 从 Context 取表的默认参数。同参数复用、异参数新 channel（或显式报错）。

**验收**：`test_file_logger_respects_run_dir`。

#### F-23 调度器收尾与协程回调

1. `close()` 追踪并取消全部 `call_later` 句柄（现状短延迟定时器残留，scheduler.py:140-141）；
2. 协程回调 `create_task` 持强引用 + done-callback 记异常（消灭"Task exception was never retrieved"）；
3. `call_repeating(interval<=0)` 直接 `ValueError`（现状钳成 0 退化为忙循环）；
4. `pending_count()` 语义修正：= 短路径 + wheel 条目之和，docstring 写明；
5. `Scheduler.loop` 懒初始化删除（强制 `bind_loop`，消灭弃用的 `get_event_loop()` 语义，scheduler.py:74-77）。

#### F-24 事件总线类型化收尾

1. 订阅匹配支持**子类**（`isinstance(event, T)` 语义，typed event bus 的常规期望）——顺带消灭死代码 `StartupContextEvent`（要么让基类订阅可用，要么删除，选前者）；
2. `EventHandler` 收紧为 `Callable[[E], Awaitable[None] | None]`（泛型 Protocol），`emit` 类型签名补泛型；
3. 慢 handler 保护：可选 `handler_timeout`（默认关，避免改变语义），超时记日志不取消（游戏逻辑里取消业务 handler 危险）；
4. 新增 `EnvReadyEvent`（见 2.4）。

#### F-25 游戏时钟

1. **编号基数保持旧仓**（GetDayNo/GetWeekNo 1 基、GetMonthNo 0 基）——持久化数据兼容（写进 docstring 与文档，评审指出的"不一致"是旧仓契约，必须保留）；
2. 时区显式化：`clock.tz: str = "Asia/Shanghai"`（zoneinfo，官方推荐），日/周边界用配置时区计算（消灭 DST 漂移与跨机不一致）；
3. tick 延迟补偿：边界扫描改为"补发错过的所有整点/半点"（现状 tick 延迟 >60s 该小时事件整个丢失，runtime.py:285-296）——按上次触发时间到 now 的闭区间逐点判定；
4. `NewWeekEvent` 字段名 `week_day` 改 `week_no`（现恒传 1 语义混乱；旧仓同源问题一并根治——`week_day` 拆出真正的 `GetWeekDay()` 语义由 com_time 提供）。

#### F-26 配置层收尾

数字键重复检测（`"1"`/`"0001"` 报错而非覆盖）；secrets 路径声明式收集（`model_fields` 扫描 `SecretRef` 类型，新加密钥字段零遗漏——消灭 `_SECRET_PATHS` 手工同步）；`process_index_of` 未知类型抛 `ValueError`；`$plain:` 空值语义修正（允许空串=no-auth，与 models.py:64 文档一致）。

#### F-27 控制台归属与细节

终端 console 仅主进程（默认）；`￥` 前缀命令；时钟事件写 `clock.log`。

#### F-28 可观测全量接线

现状：十几个指标定义后仅 loop_latency 有观察点；无导出端点。

**方案**：所有定义指标在自然埋点接线（rpc 调用/超时/延迟、save 队列/成功/失败/丢弃→改"退避"、reload 次数/失败、connection 计数、dispatch 错误、ipc unroutable）；主进程起 `prometheus_client.start_http_server(metrics_port)`（官方 API，默认 9100，可关）；告警回调接口 `obs.alarms.register(name, callback)`（save 连续失败/队列深度/loop 延迟/RPC 超时率四个内置告警源）。这是问题#21 的完整闭环。

---

### 3.4 M4 热更防护网（F-29/F-30）

#### F-29 就地热更四重加固

现状缺陷：沙箱在同一解释器 exec 模块顶层（副作用双跑，inplace.py:97-110）；不校验函数签名（:153-160 只比 co_freevars）；沙箱与 reload 两次读文件（TOCTOU，:100 vs :63）；类字典部分更新后回滚不可撤销（:305-307 只恢复模块 dict）；`__eq__/__hash__/__slots__` 变更无守卫（:146-147）。

**方案**（每条给业界依据）：

1. **沙箱去执行化**：预验证改为 `ast.parse` + 纯静态结构比对（禁止项检测），**不再 exec 新模块**——Python 官方对 `importlib.reload` 的警告清单（reload 不更新旧引用/需保持 API 兼容）转化为静态检查规则。若业务想要"可执行级"验证，提供 opt-in 的子进程试跑（`multiprocessing` 隔离进程 exec 新代码，副作用不落本进程），默认关闭；
2. **签名校验**：`inspect.signature(old) != inspect.signature(new)` 且 `not new_is_backward_compatible` → 拒绝热更（报告精确 diff：参数增删/默认值变化/keyword-only 变化）。向后兼容判定：新签名能接受旧签名全部调用形态（新增参数必须带默认值）。这是线上热更事故（加必选参数 → 全线 TypeError）的标准防线；
3. **单次读源（消灭 TOCTOU）**：读一次源码 bytes → `compile` 得 code object → 沙箱 AST 检查与真实更新**共用同一份字节码**（自实现 `reload_from_code(module, code)`：`exec(code, module.__dict__)` 替代 `importlib.reload` 的二次文件读）；
4. **真回滚**：`ModCache` 快照扩展为"模块 dict + 每个受影响类的 `__dict__` 浅拷贝 + 每个函数的旧属性"，异常时逐对象恢复；
5. **dunder 守卫**：`__eq__/__hash__/__slots__/__init__ 签名` 变更默认拒绝（`allow_dunder` 显式豁免清单）——`__hash__` 变更会让已存于 dict/set 的实例查找错乱；
6. **实例迁移辅助**：`ReloadedClass` 映射返回给调用方 + 新增类属性自动以 `__reload_default__`（类可声明的默认值字典）回填旧实例——替代旧仓"全靠业务 `__reload__` 手工补"；
7. **属性级 `__reloadkeep__`**：值对象自带 `__reloadkeep__` 标记也保留（旧仓 reload.py:145-146 两形态之一，新仓缺）；
8. **热更提交仍同步**（compile+exec 在 loop 内）但记录耗时指标；超阈值告警（大模块拆分提示）；
9. 禁忌清单成文 `docs/hot-reload.rst`（从旧仓 reload.py:5-33 头注释迁移扩充：继承/super/闭包结构/元类/C 扩展/__slots__/签名不兼容/dunder）。

**验收**：`test_reload_rejects_signature_change`、`test_reload_rejects_hash_change`、`test_reload_no_double_side_effects`（沙箱不执行：模块级计数器只 +1）、`test_reload_single_source_read`（两次保存竞态下校验与执行同源）、`test_reload_class_rollback_restores_dict`、`test_reload_backfills_new_attributes`。

#### F-30 协议注册表热更钩子

对齐旧仓 `Network.__reloadkeep__=("m_RefInstance",)` + 自动 `ReInitHandlers`：gateway 的 handler 注册表在模块热更后对**活实例**重跑绑定（`@rpc`/业务协议均适用）；注册表本身挂 `PreReload/OnReload` 事件。

---

## 4. 功能补齐设计

### 4.1 业务门面包 `pyline.api`（主计划 P5 承诺，未落地）

设计原则：门面是**薄层**——只做转发与默认参数注入（从 Context 取实例），零业务逻辑，全部从 `pyline.Context` 显式获取（规范#2：无模块级单例）。业务入口统一：

```python
from pyline import api   # template main.py 里 api.bind(ctx) 一次
api.rpc.call(20001, "game.shop.buy", uid=1)
```

模块划分与旧仓语义映射（全部 async，mypy strict）：

| 门面 | 内容 | 对应旧仓 |
|---|---|---|
| `api.env` | `service_no()/service_name()/is_main()/is_db_process()/is_single()/is_develop()/server_no_of(service_no)/shutdown(reason)/kill(reason)/main_pid()/platform()` | aio_core + aioinfo 判定族 |
| `api.registry` | `server_list(exclude)/server_ip(no)/server_port(no)/server_name(no)/server_by_ip(ip)/proxy_list()/is_proxy(no)` | GetServerList 族 |
| `api.task` | `spawn(coro, *, name)/start_task(coro)/add_start_wait(flag) -> release/on_quit(coro)` | CreateTask/CreateStartTask/AddStartWait/QuitTask |
| `api.timer` | `TimerFacade`：`call(flag, delay, func, *a)/ms_call/soon_call/left(flag)/delete(flag)`——语义逐条对齐旧仓（同 flag 重设覆盖、0.001s 下限、协程/普通双支持） | NewTimer |
| `api.rpc` | `call(srv, path, *a, timeout)/notify(srv, path, *a)/current_caller()` | RpcCall/RCB/RpcFunc/RpcFromServer |
| `api.db` | `execute(sql, *args)/query(sql, *args)`（本地/远程自动路由）+ 常量 `MYSQL_INT/STR/TEXT/DATA` | MysqlExecute/MysqlQuery |
| `api.redis` | `get/set/delete/delete_many` | RedisSet/Get/Del/MultiDel |
| `api.orm` | `DataSaver/TrackableModel/TrackedDict/TrackedList` + saver 便捷方法别名（`flush_now(force)/mark_dirty()/delete()/is_loaded()/is_deleted()/add_load_hook(fn)`） | ColumnSave/RowSave API 族 |
| `api.clock` | `now()/now_int()/day_no()/week_no()/month_no()/set_debug_time(t)/push_debug_time(s)/format(...)` | com_time 的框架侧子集 |
| `api.debug` | `trace(msg)/format_exception(exc)` | TraceMsg/RaiseError 的格式化能力（无全局劫持） |
| `api.coverage` | `start()/stop()/save()`（4.5） | StartCoverage/StopCoverage |
| `api.log` | `file(name)/file_debug(name)`（写 `{log_dir}/{进程类型}/...`） | LogFile/LogDebug |

`.pyi` 自动生成（问题#16 根治）：`scripts/gen_stubs.py` 用 `griffe`（mkdocstrings 的官方解析器）从真实 API 生成存根并入 CI 校验"存根与实现同步"。

### 4.2 类型化协议注册（替代旧 SocketNet）

业务协议不再手写 `Packet*` 字节流，改为类型化消息 + 装饰器注册（保留旧仓"子协议 handler 注册 + 重复检测 + 热更重绑"语义）：

```python
@rpc_handler("game.shop")          # flag 级注册，重复注册 ValueError（对齐旧 Gateway）
async def shop_call(msg: ShopRequest, *, caller: int) -> ShopReply: ...
```

msgpack 结构化消息 = `dataclass + dict codec`（`db/serialization.py` 已有 dataclass_codec 复用）。客户端协议（连接级）由 gateway 按 flag 分发到注册表；`connection` 句柄随 `ClientConnectedEvent` 下发（2.4）。

### 4.3 Tracked 容器（补 `touch()` 之外的旧仓人体工学）

`TrackedDict[K,V]/TrackedList[V]`：构造时绑定 `(owner_saver,)`，全部变更方法自动 `owner.touch()`；序列化时 `get_data()` 返回原生 dict/list。显式声明替代旧仓 co_names 字节码魔法（已批准决策），但"改容器即自动标脏"的旧语义完整保留。`mypy` 泛型完整标注。

### 4.4 template 业务公共层移植（`template/game/`）

- `com_time.py`：旧仓逐函数移植（见 2.14；**编号基数与锚点 2024-01-01 不变**）；
- `containers.py`：`TimeData/DayData/WeekData/DataOP` + 与 `TrackableModel` 的组合 mixin（`ColumnSaveOP/RowSaveOP` 等价物）；`TimeUpset` 判定 bug 修正并注释；
- `events.py`：补齐旧仓全部钩子的空实现（OnEnvInit 对应 `EnvReadyEvent` 等），三_layer 结构演示。

### 4.5 覆盖率：coverage.py 官方 API 集成

`api.coverage.start()/stop()/save()` 直接封装官方 `coverage.Coverage` API（`start/stop/save/report`，官方文档）；监控目录来自 `servers.json5` 的 `coverage_dir`（语义保留）；进程退出时自动 save 到 `{log_dir}/.coverage.{进程类型}`（`Coverage(data_file=...)` + `combine()` 合并多进程数据——官方多进程测量方案）。自研 trace 机制不迁移（决策#13）。

### 4.6 启动横幅与完成日志

structlog 多行 banner（项目名/服务名+号/绑定地址/Python 版本/模式 develop|production）+ 启动完成打印 `Server -> ip:port` / `Client -> ip:port`（旧 FrameShowFinish 细节）。不引入 pyfiglet（附录 A.5）。

---

## 5. 测试与验收策略

| 层 | 内容 | 工具/标记 |
|---|---|---|
| 单元 | 全部 F 编号修复的验收用例（上文逐条列出，约 60 个新用例） | pytest |
| 集成-DB | 真实 MySQL：首启建库→建表→版本表→迁移→drift 检测→存盘重试→关停冲刷 | `@pytest.mark.mysql`（**让 CI 已拉起的容器第一次真正工作**） |
| 集成-Redis | 真实 Redis：set/get/del/multi_del、超时、重连 | `@pytest.mark.redis` |
| 集成-进程 | supervisor：spawn/崩溃/宽限/父亡子退/信号优雅退出 | `@pytest.mark.integration` |
| 语义差分 oracle | 以旧仓为 oracle 的**语义**差分：帧协议（粘包/半包/恶意长度）、RPC（成功/超时/无函数/错误）、存盘 SQL 形态（UPSERT 语句字节级一致）、时钟编号（同输入同输出）、配置解析（旧 JSON 样例转 JSON5 后行为一致） | 旧仓只读引用，不迁移代码 |
| 混沌 | 慢消费者、对端断连、DB 闪断 30s（验证 F-01 不丢数据）、ZMQ 断连重连、proxy 即断重连、热更失败回滚、畸形帧风暴 | pytest-asyncio |
| Soak | 72h 长跑 + tracemalloc 堆对比（任务/pending/连接零增长）；性能基线：RPC P99、IPC 吞吐、万级定时器 tick 开销进 CI 报告（不设硬门禁，回归 >20% 告警） | 脚本 + CI nightly |
| 覆盖率门禁 | 65% → **85%**（core/net/db/reload ≥90%，主计划 P0 原始承诺） | coverage + CI fail-under |
| 文档 | mkdocs + mkdocstrings 站点：架构/启动时序/消息流、API 参考、热更禁忌清单、运维手册（配置/发布/回滚/迁移工具）、旧→新 API 迁移对照表 | mkdocs |

---

## 6. 里程碑与执行顺序

依赖关系：M1（数据安全）独立可先行；M2 依赖 F-12/F-13 先行（协议正确性）；M5 门面依赖 M1-M3 的 API 稳定；M6 收尾。

| 里程碑 | 内容 | 规模估算 | Done 标准 |
|---|---|---|---|
| **M1 数据安全** | F-01..F-11（存盘永不丢、建库顺序、版本迁移、迁移链+工具、SQL 加固、keepalive、redis） | ~2 周 | mysql 标记集成测试全绿；旧库数据经迁移工具转换后读写一致；DB 闪断 30s 混沌用例零丢失 |
| **M2 网络健壮** | F-12..F-18（分块统一、分发隔离、RPC 清理、IPC 单写者、proxy 身份、连接补全） | ~2 周 | 畸形帧风暴不断链；慢 DEALER 不堵总线；>1MB RPC 往返正确 |
| **M3 内核收尾** | F-19..F-28（启动 fail-fast、信号、supervisor、log/调度/事件/时钟/配置/console/metrics） | ~1.5 周 | 启动期关停不挂死；主进程 Ctrl+C 优雅退出；指标端点可见全部指标 |
| **M4 热更防护** | F-29/F-30（AST 沙箱、签名校验、单源读、真回滚、dunder 守卫、注册表钩子、禁忌文档） | ~1.5 周 | 签名/dunder 变更被拒且报告 diff；热更全程零副作用双跑 |
| **M5 门面+模板** | pyline.api 全模块、Tracked 容器、template com_time/containers、coverage 集成、stub 生成 | ~2 周 | 旧仓 script 层示例业务在 template 上以新 API 重写并通过（迁移演练）；stub 与实现 CI 同步校验 |
| **M6 测试文档发布** | CI 门禁 85%、soak/混沌基线、mkdocs、v1.0-RC tag | ~1.5 周 | CI 全绿（含 mysql/redis 集成 job 实质化）；文档站可读；CHANGELOG + 语义化版本 |

合计约 10-11 周（单人全职口径，含 20% 缓冲）。

**提交纪律**：每个 F 编号独立 commit（`fix(F-xx): ...`），里程碑合并前全量 `ruff+mypy+pytest` 门禁；不引入任何新的第三方依赖（banner/stub/coverage 均用既有或官方标准库能力，griffe 仅入 dev 依赖组）。

---

## 7. 风险与决策记录

| 风险 | 缓解 |
|---|---|
| 旧库存量 pickle 数据迁移（生产数据） | 迁移工具默认 dry-run；先备份表再 `--execute`；工具本身有往返校验（读回比对） |
| `loads_migrated` 接线后旧版本 blob 首次加载触发热路径 | 迁移工具在上线前统一转换；运行时迁移链仅兜底 |
| RPC arity 校验改变对端行为 | 只拒畸形帧不断链，日志可观测；内部消息面自控 |
| 热更签名校验过严挡住合法热更 | 兼容性判定只挡"必选参数新增/删除/默认值移除"；显式 `@reload_allow(...)` 豁免装饰器 |
| 单写者 IPC 改造引入新竞态 | 乱序/背压/慢对端三类专项测试先行；保留旧路径开关一个版本（`ipc_legacy=true`）观察 |
| Windows 信号语义差异（SIGTERM/SIGHUP） | Proactor 下仅处理 SIGINT；SIGTERM 走 supervisor 显式 terminate；CI 双平台跑集成 |
| 事件总线支持子类分发的性能 | 匹配用 `type(event).__mro__` 缓存（注册表小，实测无影响；有 benchmark 用例） |

---

## 附录 A：不迁移项清单（决策依据）

1. **共享内存 IPC + `shrmem.max_mem` eval** → 决策#2：纯 ZMQ（单槽覆盖丢数据不可修复）。
2. **print/settrace/excepthook/warnings/logging 全局劫持、SetPrintHook** → 决策#13：structlog + `logging.captureWarnings` 官方等价物；调试能力改为 `api.debug`（4.1）。
3. **`CallEvent` 返回值 or-链** → async 广播语义下无意义；请求-响应用 RPC。
4. **pyinstaller 打包 / 编译模式 / `IsBootWithCode` / `freeze_support`** → 决策#8：uv 锁定 + 目录分发。
5. **pyfiglet ASCII banner** → 纯装饰依赖；structlog banner 保留信息量（服务名/号/地址/版本）。
6. **`frameapi/` 手写存根** → 问题#16：griffe 自动生成，杜绝脱节。
7. **`Packet*` 手工字节打包 API 字节级兼容** → 决策#10：统一 msgpack 结构化消息；协议是框架内部实现，无外部兼容包袱；差分测试做语义级。
8. **无 DB 权限进程 SQL 静默成功（`func()`/`func(())`）** → fail-fast 原则：显式 `ConnectionError`（静默丢 SQL 掩盖配置错误）。
9. **旧 `TimeUpset` 键/时长混用 bug、`GetServerList` 排除分支 TypeError 等已知 bug** → bug 非功能，按修复后语义迁移并记录。
10. **自研 coverage trace** → 决策（主计划六.3）：coverage.py 官方 API。
11. **`m_AutoLoad`（旧仓声明未使用）** → 死代码不迁移；自动加载语义已是新仓默认。
12. **`TimeWheel` 类** → 决策#14：并入 Scheduler。

## 附录 B：方案依据的官方/业界资料

- **ZeroMQ**：zguide Chapter 4（ROUTER 非阻塞/慢消费者隔离/队列代理 per-worker 管道）；libzmq `zmq_socket(3)`（ROUTER HWM 静默丢弃、`ZMQ_ROUTER_MANDATORY` EHOSTUNREACH）。
- **asyncio**：官方文档 `loop.add_signal_handler`、TaskGroup（3.11+）、`asyncio.to_thread`、取消语义与 `wait_for`；社区经典 Graceful Shutdowns with asyncio（Lynn Root）。
- **MySQL**：8.0 Online DDL 官方文档（`ALGORITHM=INSTANT, LOCK=NONE`、INSTANT 限追加列）；Alembic `alembic_version` 单行版本表模式；expand-contract（parallel change）模式。
- **coverage.py**：官方 API 文档（`Coverage/data_file/combine` 多进程测量）。
- **redis-py**：官方 asyncio 文档（`socket_timeout/health_check_interval/retry_on_timeout`、连接池自动重连）。
- **pydantic**：官方 `Literal` 约束与 `extra="forbid"` 校验模式。
- **Python 标准库**：`importlib.reload` 官方警告（旧引用不更新——就地更新算法的存在依据）；`inspect.signature` 比较官方用法；`pickle` 受限加载安全建议；`zoneinfo` 时区；`weakref.WeakSet`。
- **旧仓**：`F:\ServerLite`（行为规格 oracle，只读）；`docs/plan.md`（已批准主计划与其 22 项问题映射）。
