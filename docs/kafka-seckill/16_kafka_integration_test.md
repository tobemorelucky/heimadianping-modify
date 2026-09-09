# Kafka 本地联调报告

## 1. 联调目标与隔离边界

本次验证本地 KRaft Kafka、Kafka Client 2.5.1 与 Spring Kafka 2.5.14 的基本集成能力，包括集群健康检查、Topic 管理、JSON 消息发送、消息消费和测试消费组 offset 提交。

联调严格隔离于正式秒杀链路：

- 不修改、不调用 `VoucherOrderServiceImpl` 秒杀入口。
- 不修改、不切换 Redis Stream。
- 不启动 Spring Boot 应用上下文和正式 `@KafkaListener`。
- 只使用 `hmdp.seckill.order.integration-test.v1` 专用测试 Topic。
- 每次使用随机测试 Consumer Group，不读写正式消费组 offset。
- 测试代码只存在于 `src/test/java`，由环境变量显式开启。

## 2. Kafka Docker 环境检查

Compose 文件检查：

```bash
docker compose -f docker/kafka/docker-compose-kafka.yml config --quiet
```

容器状态检查：

```bash
docker compose -f docker/kafka/docker-compose-kafka.yml ps
```

本次实测结果：

```text
NAME         IMAGE                SERVICE   STATUS   PORTS
hmdp-kafka   apache/kafka:3.9.2   kafka     Up       0.0.0.0:9092->9092/tcp
```

环境结论：`hmdp-kafka` 已运行，宿主机 9092 端口已正确映射，KRaft 数据使用 `hmdp-kafka-data` 命名卷持久化。

## 3. Kafka 健康检查

### 3.1 容器级检查

```bash
docker compose -f docker/kafka/docker-compose-kafka.yml ps
```

`STATUS` 应为 `Up`。该检查只能证明容器进程存在，不能单独证明 broker 已能处理请求。

### 3.2 Broker API 检查

```bash
docker exec hmdp-kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

成功时应输出 broker 节点和支持的 API 版本。本次实测首行为：

```text
localhost:9092 (id: 1 rack: null) -> (
```

### 3.3 Spring Kafka AdminClient 检查

独立联调测试会调用 `AdminClient.describeCluster()`，要求在 10 秒内取得 clusterId 和至少一个 broker。实测日志：

```text
KAFKA_INTEGRATION_HEALTH_OK clusterId=4L6g3nShT-eMCtK--X86sw, brokerCount=1, bootstrapServers=127.0.0.1:9092
```

推荐本地排查顺序：Compose config → 容器 `Up` → Broker API 成功 → Spring Kafka AdminClient 成功。

## 4. 独立测试代码

测试类：

```text
src/test/java/com/hmdp/kafka/integration/KafkaLocalIntegrationTest.java
```

默认执行普通 `mvn test` 时，测试因未设置 `KAFKA_INTEGRATION_TEST_ENABLED=true` 而跳过，避免 CI 或无 Kafka 开发机被外部环境阻塞。

### 4.1 测试 Producer 方法

`sendTestOrderMessage()` 使用独立创建的 `KafkaTemplate<String, VoucherOrderMessage>`：

1. 发送目标固定为专用测试 Topic。
2. 使用 `userId` 字符串作为 key。
3. 使用 Spring Kafka `JsonSerializer` 序列化消息。
4. 配置 `acks=all`。
5. 最多等待 10 秒获取 broker 发送结果。
6. 记录 Topic、partition、offset 和订单字段。

实测发送日志：

```text
KAFKA_INTEGRATION_MESSAGE_SENT topic=hmdp.seckill.order.integration-test.v1, partition=0, offset=0, orderId=1788750496814, userId=920250907, voucherId=930250907
```

### 4.2 测试 Consumer 验证流程

测试 Consumer 使用独立 `DefaultKafkaConsumerFactory`：

```text
订阅专用测试 Topic
↓
使用随机测试 Consumer Group
↓
JsonDeserializer 反序列化 VoucherOrderMessage
↓
轮询并按本次 orderId 定位消息
↓
校验 orderId、userId、voucherId、createTime
↓
手动 commitSync 测试消费组 offset
```

实测消费日志：

```text
KAFKA_INTEGRATION_MESSAGE_CONSUMED topic=hmdp.seckill.order.integration-test.v1, partition=0, offset=0, key=920250907, orderId=1788750496814, userId=920250907, voucherId=930250907
```

发送与消费的 Topic、partition、offset、key 和订单字段一致，JSON 序列化/反序列化验证通过。

## 5. 测试执行方式

PowerShell：

```powershell
$env:KAFKA_INTEGRATION_TEST_ENABLED = "true"
$env:KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
mvn '-Dtest=KafkaLocalIntegrationTest' test
```

Bash：

```bash
KAFKA_INTEGRATION_TEST_ENABLED=true \
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 \
mvn -Dtest=KafkaLocalIntegrationTest test
```

本次结果：

```text
Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

测试结束后 ProducerFactory、Consumer 和 AdminClient 均主动关闭，Kafka 容器继续运行供后续联调使用。

## 6. Topic 检查命令

列出 Topic：

```bash
docker exec hmdp-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

本次实测：

```text
__consumer_offsets
hmdp.seckill.order.integration-test.v1
```

检查联调 Topic：

```bash
docker exec hmdp-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic hmdp.seckill.order.integration-test.v1
```

本次实测结果：

```text
Topic: hmdp.seckill.order.integration-test.v1
PartitionCount: 1
ReplicationFactor: 1
Partition: 0
Leader: 1
Replicas: 1
Isr: 1
```

后续正式三 Topic 创建完成后，可分别替换 `--topic` 参数检查：

```text
hmdp.seckill.order.create.v1
hmdp.seckill.order.retry.v1
hmdp.seckill.order.dlt.v1
```

正式三个 Topic 必须保持相同的 12 个分区，避免 Retry/DLT 按原分区转发失败。

## 7. 联调结论

- Docker Compose KRaft Kafka：通过。
- broker API 健康检查：通过。
- Kafka Client 2.5.1 与 Kafka 3.9.2 协议协商：通过。
- Spring Kafka `KafkaTemplate` JSON 发送：通过。
- Spring Kafka `DefaultKafkaConsumerFactory` JSON 消费：通过。
- userId 消息 key：通过。
- 测试消费组手动 offset 提交：通过。
- 正式秒杀主 Topic/Retry/DLT 业务链路：本阶段未写入、未切换、未验证。

当前环境已经满足下一阶段正式 Topic 创建和 Kafka 秒杀灰度联调的前置条件。
