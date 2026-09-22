# Phase P4.1：Consumer 视角 MySQL Health Observation

> 本文定义本阶段的只读观测契约。只覆盖 `hmdp-consumer` 的数据库路径与连接池；不修改订单事务、SQL、Detection、Incident 状态或自动修复。健康探针不读取用户、订单或数据库配置。

## 1. 采集边界与来源

`consumer` Profile 设置了 `spring.main.web-application-type=none`，所以不能靠普通业务 Controller 暴露健康数据。新增仅随 Consumer Profile 启动、仅绑定 `127.0.0.1` 的独立轻量 HTTP 出口 `GET /internal/aiops/mysql-health`，端口可配置，默认关闭且须显式启用。Web Profile 和原单体不启用。Agent 的 MCP Server 只访问固定本机端点，不接受模型提供的 URL、SQL 或凭据。

| 字段 | Consumer 内的数据来源 | 语义 |
| --- | --- | --- |
| `database_reachable` | 从 Consumer 实际 `DataSource` 借用连接，调用 JDBC `Connection.isValid()` | `true` 表示本次只读探测成功；`false` 仅在拿到连接但校验失败时；借连接失败、超时或未知则 `null`，不能推断 DB 已宕机。 |
| `hikari_active` / `hikari_idle` | 当前 `HikariDataSource.getHikariPoolMXBean()` | 当前瞬时连接数；池未初始化或非 Hikari 时为 `null`。不通过 Agent 主机另建连接替代 Consumer 池。 |
| `connection_timeout_count` | 当前项目未配置可验证的 Hikari 累计 Timeout 指标出口 | 本阶段为 `null` / `unavailable`；不把一次探针超时伪造成累计超时数。 |
| `error_count` | 当前项目未配置统一 Consumer SQL/连接异常计数器 | 本阶段为 `null` / `unavailable`；不以日志行数冒充准确计数。 |

端点响应固定 JSON：`source_role=hmdp-consumer`、`collected_at`、`database_reachable`、`connection_test_status`、`hikari_active`、`hikari_idle`、上述两个可空计数及 `unavailable_metrics`。不返回 JDBC URL、用户名、密码、SQL、表名、用户或订单数据。端点只接受 GET；其他方法拒绝。探针执行 `getConnection()` / `isValid()` / `close()`，无任意 SQL 接口、无数据库写入。使用有界短时探针，避免数据库故障把出口长期阻塞；不可用时仍可尽量返回池快照。

## 2. MCP Tool 契约与权限

新增 `get_mysql_health`：Tool Registry / Manifest 固定 `READ_ONLY`、`category=observation`、`risk_level=none`、`approval_required=false`。输入只允许可选 `incident_id`（用于关联），不允许目标角色、URL 或查询语句由模型指定。MCP 子进程从受控环境配置读取固定 `http://127.0.0.1:<port>/internal/aiops/mysql-health`；仅允许 loopback、固定路径和 HTTP，禁止重定向，避免 SSRF 或越权访问。

输出复用 `ToolObservation`：`kind=mysql_health`、`source_tool=get_mysql_health`、`source_role=hmdp-consumer` 位于 `data`；`source` 标明 Consumer 健康出口。原始数据引用不含密码或用户数据。MCP 只转译 Consumer 的观测事实，不宣称“MySQL 是根因”。Planner 可在后续诊断中选择该 Tool，但本阶段不添加 MySQL Detection 或恢复规则。

## 3. 失败语义

| 情形 | Observation `status` / `completeness` | 处理 |
| --- | --- | --- |
| 探针通过且 Hikari 指标存在 | `partial` / `partial` | `database_reachable=true` 与 active/idle 有效，但两个累计计数仍 `null`；不能假装全量指标已齐。 |
| 数据库不可达或连接校验失败 | `partial` / `partial` | 保留可得池指标，`database_reachable=false/null` 与 `connection_test_status` 明确区别；这是受限证据，不是任意 SQL 失败。 |
| 指标源缺失、池未初始化 | `partial` / `partial` | 缺项为 `null` 且列在 `unavailable_metrics`；不填 0。 |
| Consumer 角色不匹配、响应非法、端点拒绝 | `error` / `unknown` | 拒绝作为 MySQL 事实；仅保留结构化错误。 |
| MCP 到本地出口的请求超时 | `timeout` / `unknown` | 仅说明本次观测超时；不能推断数据库故障。 |

若 Consumer 端的有界探针超时但 HTTP 自身返回了 JSON，可作为 `partial` 保留池指标和 `connection_test_status=timeout`；若连 HTTP 也超时，MCP 返回 `timeout`。调用时限应小于 Runtime Tool 总时限。所有异常摘要须脱敏，不传播 JDBC 地址、凭据或订单信息。

## 4. 安全与验收

- 默认关闭、仅 `consumer` Profile、仅 IPv4 loopback；不经 Nginx、不开放 Docker 端口。固定端口在本地启动前核对，Agent 和 Consumer 使用同一配置。
- 拒绝非 GET、非预期角色和非固定端点；Manifest 校验及 Permission Gateway 保持只读。探针不接收 SQL，也不执行写操作。
- 测试使用假 DataSource 与本机模拟 HTTP，不停止共享 MySQL：覆盖连接成功、连接失败、池指标缺失、角色错误、超时、Manifest 只读和 MCP 调用。
- 真机验收须单独证明：Consumer 与 Agent 读取同一路径；正常时池数来自 Consumer；停用隔离 MySQL 或阻断 Consumer 专属路径时返回结构化观测。若仅运行单元测试，不宣称真实 MySQL 故障链路已通过。
