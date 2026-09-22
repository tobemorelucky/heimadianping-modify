# Kafka Consumer Down 真实故障演示（Phase P2.2）

本演示只在本机、隔离 Topic/Group、带 `AIOPS_P22_` 标记的测试券与测试账号上执行。订单始终走 **HTTP 秒杀入口 → Redis Lua 准入 → Web Kafka Producer → Kafka Topic → Consumer → MySQL**；辅助脚本不直接向 Kafka 生产消息、不直接写订单表。只停止本次启动的 Consumer JVM，Web 和 Docker 依赖保持运行。不包含自动修复或手工创建 Incident。

## 安全边界与准备

- 先确认没有原单体 JVM 或其他消费者加入演示 Group，8081 未被占用。不要在共享生产数据库、真实券或真实用户上运行。
- 记录本机 `mysql`、`redis`、`kafka` 容器原始状态，只启动缺失的依赖：`docker compose up -d mysql redis kafka`。结束后仅恢复**本次改变的**容器状态，不清空 Docker volume。
- 用 Java 17 启动 Consumer 时，当前 MyBatis-Plus 3.4.3 需要 JVM 参数 `--add-opens=java.base/java.lang.invoke=ALL-UNNAMED`。缺失它会触发 `SerializedLambdaMeta` 的 `InaccessibleObjectException`，消息可能进入 DLT；这不是 Consumer Down 故障。Web 在本轮未需要该参数。不要把 DLT 消息算作正常落库。
- 为演示创建独立的 1 分区主、Retry、DLT Topic。主 Group 必须先消费一笔基线订单、形成 committed offset，之后才能准确比较 lag；没有 committed offset 时不能把未知 lag 当作 0。
- `Invoke-SeckillDemoOrders.ps1` 仅接受 `127.0.0.1:8081`/`localhost:8081`，需要显式确认开关。它调用 `/user/code`、从本机 Docker Redis **只读**获取该测试账号验证码、调用 `/user/login`，再调用 `/voucher-order/seckill/{id}`；不会打印验证码或 token。首次运行可经 `/voucher/seckill` 创建标记测试券，不绕开业务入口。

以下命令在仓库根目录执行。不要在用户已有 Topic/Group 上复用这些名称；日期后缀可按本次演示调整，并在所有 JVM 和 Agent 参数中保持一致。

```powershell
docker compose up -d mysql redis kafka
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic hmdp.aiops.p22.20260921.main --partitions 1 --replication-factor 1
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic hmdp.aiops.p22.20260921.retry --partitions 1 --replication-factor 1
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic hmdp.aiops.p22.20260921.dlt --partitions 1 --replication-factor 1
```

## 双 JVM 启动与真实订单

先在**终端 A**启动 Consumer，再在**终端 B**启动 Web。不要同时启动原单体。两个 JVM 的主、Retry、DLT Topic 必须一致；以下命令故意使用独立 Group。

```powershell
# 终端 A：只运行 Consumer，后续只在这个终端 Ctrl+C
java --add-opens=java.base/java.lang.invoke=ALL-UNNAMED -jar .\target\hm-dianping-0.0.1-SNAPSHOT.jar --spring.profiles.active=consumer --hmdp.kafka.topics.voucher-order=hmdp.aiops.p22.20260921.main --hmdp.kafka.topics.voucher-order-retry=hmdp.aiops.p22.20260921.retry --hmdp.kafka.topics.voucher-order-dlt=hmdp.aiops.p22.20260921.dlt --spring.kafka.consumer.group-id=hmdp-aiops-p22-20260921-main --hmdp.kafka.consumer.retry-group-id=hmdp-aiops-p22-20260921-retry --hmdp.kafka.consumer.dlt-group-id=hmdp-aiops-p22-20260921-dlt

# 终端 B：HTTP + Producer；不装配 Kafka Listener
java -jar .\target\hm-dianping-0.0.1-SNAPSHOT.jar --spring.profiles.active=web --canal.enabled=false --hmdp.kafka.topics.voucher-order=hmdp.aiops.p22.20260921.main --hmdp.kafka.topics.voucher-order-retry=hmdp.aiops.p22.20260921.retry --hmdp.kafka.topics.voucher-order-dlt=hmdp.aiops.p22.20260921.dlt --spring.kafka.consumer.group-id=hmdp-aiops-p22-20260921-main
```

先请求 `http://127.0.0.1:8081/shop-type/list` 确认 Web 可用；确认 Consumer 日志有主 Topic 分区分配且没有 HTTP 端口。用一个测试用户创建测试券及基线订单，记录脚本返回的 `voucher_id` 和 `order_ids`：

```powershell
$baseline = .\scripts\aiops-demo\Invoke-SeckillDemoOrders.ps1 -Phase baseline -Phones @('19920921004') -Stock 10 -IUnderstandLocalTestData | ConvertFrom-Json
$voucherId = $baseline.voucher_id
$baseline
```

HTTP 返回订单号仅表示请求受理；必须查询 MySQL 中相同券 ID 的订单数，以及 Kafka committed offset 和 lag，确认消息已真正落库。下面的 `<voucher_id>` 使用上一步输出，不要查询或修改非测试券：

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group hmdp-aiops-p22-20260921-main
# 使用本机 MySQL 客户端连接 compose 映射端口 3307，执行只读 SQL：
# SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=<voucher_id>;
# SELECT stock FROM tb_seckill_voucher WHERE voucher_id=<voucher_id>;
```

只在终端 A 按 `Ctrl+C` 停止 Consumer；若后台运行，先核实确切 PID/启动命令，再对该 PID 使用 `Stop-Process -Id <consumer-pid>`。等待 Kafka 会话超时，查询应显示 `has no active members`。终端 B 保持运行，使用**不同的测试账号**再发送 2–3 笔：

```powershell
.\scripts\aiops-demo\Invoke-SeckillDemoOrders.ps1 -Phase consumer_down -VoucherId $voucherId -Phones @('19920921005','19920921006','19920921007') -IUnderstandLocalTestData
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group hmdp-aiops-p22-20260921-main
```

验收需同时看到：Web 返回新的订单号、主 Topic 的 `LOG-END-OFFSET` 增长、主 Group 无成员且 `LAG` 增长、MySQL 的该券订单数暂不增长。随后重启终端 A 的同一 Consumer 命令，确认 lag 降为 0，MySQL 订单数追上；这一步是演示环境恢复，不是 Agent 自动修复。测试券、订单和隔离 Topic 留在本地供追溯，不主动删库或清理 Topic。

## 接入现有 Monitoring（不手工创建 Incident）

在 `aiops-agent/` 启动现有独立 Scheduler，使用**独立 SQLite 文件**与和本次演示一致的 Topic/Group。演示只产生少量订单，故将规则阈值临时设为 1；规则仍要求两个连续成功窗口。真实环境不可直接使用此低阈值。先在健康基线采集一次，再保持持续巡检，随后停 Consumer 并下单。

```powershell
cd .\aiops-agent
$env:AIOPS_DATABASE_PATH = (Join-Path (Get-Location) 'data/p22_real_20260921.db')
$env:AI_MODEL_PROVIDER = 'mock'
$env:KAFKA_VOUCHER_ORDER_TOPIC = 'hmdp.aiops.p22.20260921.main'
$env:KAFKA_CONSUMER_GROUP = 'hmdp-aiops-p22-20260921-main'
$env:AIOPS_KAFKA_LAG_THRESHOLD = '1'
.\.venv\Scripts\python.exe -m monitoring.scheduler
```

只有当 `get_kafka_status` 连续返回 `success`、观测到 `member_count=0` 且 `lag>1` 时，才验收 `Observation → AnomalySignal → Incident → Diagnosis`。此时用现有只读 Console API/页面核对：Incident 的 trigger signal、Skill 选择/加载、Tool Calls、Evidence、Hypothesis 更新、Report 和 Trace Replay。**不能**用 `POST /incidents`、Fixture 或 `console_demo` 代替自动触发。当前日志工具只读取 `target/runtime/spring-boot.out.log`/`spring-boot.error.log`；若双 JVM 日志重定向到其他文件，它会得到 partial，不能声称已验证日志证据。Console 的 Proposal 只能展示，不执行 Action。

建议截图保存位置为 `target/runtime/p22-demo/screenshots/`（本地临时演示产物，避免提交含用户数据的画面）；至少包含 Kafka CLI 的无成员/lag 页面、Console Incident Detail 的 Skill/Tool/Evidence/Hypothesis/Report、Replay。截图应遮蔽 token、手机号、验证码。**本轮因自动 Incident 未触发，未生成 Console Incident 截图。**

## 2026-09-21 本机实测记录

本轮使用独立主 Topic `hmdp.aiops.p22.20260921.main`、主 Group `hmdp-aiops-p22-20260921-main`、测试券 ID `3`、库存初始值 `10`。`target/runtime/p22-demo/` 保存本地 JVM 输出；它们不是新增日志 MCP 白名单。

| 阶段 | HTTP 已受理测试单 | Kafka 主 Group | `CURRENT-OFFSET` / `LOG-END-OFFSET` / `LAG` | 测试券 MySQL 订单数 | 测试券 MySQL 库存 |
| --- | ---: | --- | --- | ---: | ---: |
| 正常基线 | 1 | 有成员 | `2 / 2 / 0` | 1 | 9 |
| Consumer 停止后 | 累计 4（新增 3） | 无活跃成员 | `2 / 5 / 3` | 1 | 9 |
| Consumer 恢复后 | 累计 4 | 有成员 | `5 / 5 / 0` | 4 | 6 |

测试单的 HTTP 返回订单号分别为 `640080672634961922`（基线）及 `640081308290121731`、`640081308290121732`、`640081312585089029`（故障段）。该表证明真实 HTTP→Kafka 积压→Consumer 恢复→MySQL 落库，不证明 Agent 已完成诊断。

首次预检曾用测试券 ID `2` 发送 1 单，但未加 Java 17 `--add-opens`，Consumer 因 `SerializedLambdaMeta` 反射异常把该消息送入**隔离 DLT**，MySQL 订单数为 0。修正启动参数后另建测试券 ID `3` 重测；ID 2 与其 DLT 记录未清理、不纳入上表或 Consumer Down 结论。

现有 AIOps Kafka MCP 在本机 `kafka-python==2.3.2` 下两次采集（健康、故障）均返回 `error`：`KafkaConfigurationError: Unrecognized configs: {'default_api_timeout_ms'}`。故障段采集 ID 为 `col_ac647362231c4c4c84a32af3420d2cbe`；隔离 SQLite `data/p22_real_20260921.db` 中 `monitoring_collection_runs` 有 2 条 error，`anomaly_signals=0`、`managed_incidents=0`。这属于**Tool/依赖兼容失败**，不是 Kafka 健康或规则未命中。按本阶段“不修改 AIOps Agent”的限制，本轮不修复、不注入伪 Observation，也不手工创建 Incident。

真实 FaultBench 场景定义在 `faultbench/KAFKA_CONSUMER_DOWN_REAL_001.json`。它记录注入方法、健康/故障/恢复观测、期望假设和证据；当前 Agent 内 `FaultCase` 仅支持 Fixture 字段，**此文件尚未注册为可直接运行的评测用例**，不能据此宣称 FaultBench 已通过。

## 验收状态与后续解锁条件

- **已验证**：Consumer JVM 可独立停止；主 Group 从有成员变成无活跃成员；Web 继续经真实 HTTP 接受 3 笔订单；主 Topic 末端 offset 从 2 到 5，lag 从 0 到 3；故障期间 MySQL 不新增订单；恢复 Consumer 后 lag 回到 0 且 3 笔订单落库。
- **未通过/待验证**：现有 Kafka MCP 不能返回成功观测，故自动 AnomalySignal、Incident、Skill/Tool/Evidence/Hypothesis/Report 与 Console Replay 均未得到真实演示验证。日志 MCP 对分离日志的采集也未验证。
- **后续需要单独授权的修复**：调整 Kafka MCP 与当前 `kafka-python` 的参数兼容性并回归测试，随后在同样的隔离环境重跑两窗口巡检，最后核对 Console。此阶段没有改 Agent/Console/Java/Docker 架构，也没有新增 Tool 或自动修复。
