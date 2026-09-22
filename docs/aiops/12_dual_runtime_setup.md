# HMDP 双 JVM 运行说明（Phase P2.1）

同一个 `hm-dianping-0.0.1-SNAPSHOT.jar` 使用两个 Spring Profile 分别启动。`web` 提供原有 HTTP 秒杀入口及 Kafka Producer；`consumer` 启动主、Retry、DLT Kafka Listener 和订单 MySQL 事务，不启动 HTTP 服务器。未指定 Profile 时保留原单体行为。本阶段不改消息格式、Lua、订单事务、表结构、Docker 或 AIOps Agent。

## 1. 角色隔离

| 运行项 | `web` | `consumer` | 未指定 Profile（原单体） |
| --- | --- | --- | --- |
| HTTP Controller / 8081 | 可访问 | `spring.main.web-application-type=none`，不监听端口 | 可访问 |
| 秒杀入口、Kafka Producer Bean | 有 | Producer Bean 保留，供 Retry/DLT 转发使用；无 HTTP 入口 | 有 |
| 主/Retry/DLT Kafka Listener | 不创建 | 创建并启动 | 创建并启动 |
| Redis Stream 订单消费线程 | 不创建 | 不创建 | 保持原行为 |
| Canal 后台客户端 | 保持原配置 | `canal.enabled=false` | 保持原配置 |

Listener 由 `hmdp.kafka.listener.enabled` 控制，默认 true 以保持单体兼容；`web` 设为 false，`consumer` 设为 true。这里没有依赖通用 `spring.kafka.listener.auto-startup`：项目的主、Retry、DLT Listener 使用自定义 ContainerFactory，直接条件化 Listener Bean 才能确保 Web 不进入 Consumer Group。Redis Stream 消费线程也由默认 true 的独立开关隔离，避免 Web 在 Kafka 演示时仍处理旧 Stream 消息。

两个新 Profile 明确使用 `seckill.message.mode=kafka`。不要通过更高优先级的环境变量 `SECKILL_MESSAGE_MODE=redis` 覆盖它：新 Profile 中 Redis Stream 消费线程已关闭，Redis 模式不会完成异步落库；如需课程原有 Redis Stream 模式，应继续使用未指定 Profile 的单体启动。

## 2. Windows 启动

前提：Java 17、Maven、Docker Desktop 和本地依赖可用；MySQL、Redis、Kafka 地址可由原 `application.yaml` 的环境变量覆盖。先检查 8081 没有旧单体 JVM，且 Kafka 主 Group 没有旧消费者。**不要同时运行 `scripts/start-all.ps1` 启动的原单体**，否则停止新 Consumer 后 Group 仍可能有旧成员。

在项目根目录构建一次：

```powershell
mvn.cmd test
mvn.cmd -DskipTests package
```

终端 A 启动 Web：

```powershell
java -jar .\target\hm-dianping-0.0.1-SNAPSHOT.jar --spring.profiles.active=web
```

终端 B 启动 Consumer：

```powershell
java -jar .\target\hm-dianping-0.0.1-SNAPSHOT.jar --spring.profiles.active=consumer
```

两进程必须使用相同的 Kafka Broker、主/Retry/DLT Topic 配置；Consumer 的主 Group 默认 `hmdp-seckill-order-create-v1`。如使用独立测试 Topic/Group，两个终端都设置同一 `KAFKA_VOUCHER_ORDER_TOPIC`、`KAFKA_VOUCHER_ORDER_RETRY_TOPIC`、`KAFKA_VOUCHER_ORDER_DLT_TOPIC`，并给 Consumer 设置专用 `--spring.kafka.consumer.group-id=...` 及 Retry/DLT Group。推荐在隔离环境**预先创建 1 分区测试 Topic**；Broker 默认自动创建的 12 分区 Topic 若无各分区 committed offset，现有 AIOps Kafka Tool 可能返回 `partial`，无法触发完整的 lag 规则。

Consumer 无 HTTP 端口；不要为其配置第二个业务端口。要独立停止它，直接在**终端 B**按 `Ctrl+C`，终端 A 保持运行。若以后台方式运行，只对已核对命令行的 Consumer Java PID 执行 `Stop-Process -Id <consumer-pid>`；不要按模糊进程名批量结束所有 Java 进程。恢复时重新运行终端 B 的命令。当前 `scripts/start-all.ps1` 仍是旧单体启动方式，未被本阶段改动。

## 3. 验证顺序

1. 先只启动 Web。检查 `http://127.0.0.1:8081/shop-type/list` 返回 200，启动日志包含 `The following profiles are active: web`；Kafka Producer Bean 的存在由 `DualRuntimeProfileTest` 验证。专用 Consumer Group 此时不应有成员。
2. 启动 Consumer。日志应包含 `The following profiles are active: consumer` 和主 Topic `partitions assigned`，但无 Tomcat 监听 8081/其他业务端口。两进程连接同一 Kafka 地址及 Topic。查询 Group 成员：

   ```powershell
   docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group hmdp-seckill-order-create-v1 --members
   ```

   预期至少 1 个成员，且来自 Consumer JVM。若旧单体仍运行，应先停止旧实例再重测。
3. 在**隔离测试券与测试账号**上调用原 `POST /voucher-order/seckill/{id}`。HTTP 返回订单号只是“受理”；继续核验 Web 的 Broker 确认日志、Consumer 的消费/ACK 日志、Kafka offset 前进、MySQL `tb_voucher_order` 中同一 `order_id` 的实际记录。不能在共享数据上用未知券做破坏性测试。
4. `Ctrl+C` 只停止 Consumer；再次执行 Group 查询，等待 Kafka 会话超时后应显示 `has no active members`。再次访问 Web 的 `/shop-type/list` 应仍返回 200。
5. 若要验证 lag 增长，须在 Consumer 停止前确认测试 Group 对目标分区已有 committed offset；停止后经 Web 用不同测试用户产生超过基线的**有效测试消息**，对比 Topic end offset 与 committed offset。没有 committed offset 时 lag 是未知而不是 0，不能拿它验收 AIOps Detector。恢复 Consumer 后核验 lag 下降及测试订单最终状态。

`get_kafka_status` 的 `member_count` 与 `total_lag` 是只读观察。现有 Agent 日志/业务指标工具仍读取原单体 `target/runtime/spring-boot.*.log`，本阶段**未改 Agent 白名单**；如果两个 JVM 分开重定向日志，Agent 可能看不到新日志。真实双进程诊断的完整日志/业务指标适配属于后续 P2 阶段，不应把本阶段只靠 Kafka Group 的结果称为全链路诊断完成。

## 4. 本阶段测试结果与边界

- `DualRuntimeProfileTest` 验证 Web 不装配 Kafka/Redis Stream Consumer、KafkaTemplate 保留，Consumer 装配两个 Kafka Listener Bean 且配置不启动 Web，未指定 Profile 仍装配 Listener。
- `mvn.cmd test` 全量通过；构建 JAR 通过。
- 设置 `KAFKA_INTEGRATION_TEST_ENABLED=true` 后，现有 `KafkaLocalIntegrationTest` 使用专用 Topic 在真实 Broker 上验证 Kafka 发送、消费及业务入口到 Consumer 方法的消息契约；它用 Mock 事务服务，不代表真实 MySQL 落库。
- 本地烟测以独立 `hmdp.p21.smoke.20260921.*` Topic/Group 启动两 JVM：Web `/shop-type/list` 返回 200；Consumer 主 Group 显示 1 个成员；仅停止 Consumer 后 Group 显示无活跃成员，Web 仍返回 200。该测试没有向原业务 Topic 发订单。
- 单独重启 Consumer Profile 时应用成功启动且 8081 不监听，验证了它不暴露原 Controller HTTP 接口。
- **未在本轮对现有数据库执行真实订单写入，也未制造测试 Topic 的 committed offset 后注入消息。** 因此“Web 实际发送 → Consumer 实际落库”和“停 Consumer 后 lag 增长”仍是隔离测试券/Topic/数据库下的待执行验收项，不能以启动烟测冒充已通过。

停止演示后确认两个 Profile JVM 均已退出，避免与原单体并行。Kafka 烟测使用的独立测试 Topic/Group 可能留在本地 Broker；不要删除旧 Topic、消费组或数据卷来清理它们。下一阶段若要自动 Incident，先按 `11_real_fault_environment_design.md` 准备完整 offset、少量受控消息和 Agent 巡检阈值，再验收 Monitoring → Incident → Diagnosis → Replay。
