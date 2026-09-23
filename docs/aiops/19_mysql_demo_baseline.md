# AIOps Demo 双 JVM 正常链路基线

## 1. 目的与边界

本文记录 Phase E2.0 在本地 Windows 环境中的正常链路接入与实测结果。`hmdp-web` 和 `hmdp-consumer` 使用同一份应用代码，通过组合 Spring Profile 连接独立 AIOps Demo MySQL、Redis 和 Kafka。

本阶段没有停止 MySQL、没有注入连接故障、没有修改 Detector、没有创建 Incident，也没有执行修复动作。

实测日期：2026-09-23（Asia/Shanghai）。

## 2. 运行拓扑与隔离标识

```text
HTTP :8081
  -> hmdp-web (web,web-aiops-demo)
  -> Redis Lua 127.0.0.1:16379
  -> Kafka 127.0.0.1:29092
     topic: hmdp.aiops.mysql-demo.order.main.v1
  -> hmdp-consumer (consumer,consumer-aiops-demo)
     group: hmdp-aiops-mysql-demo-consumer-v1
  -> MySQL 127.0.0.1:13307 / hmdp
```

Demo 资源与原环境通过端口、Compose project、volume、Topic 和 Consumer Group 同时隔离：

| 资源 | Demo 标识 |
| --- | --- |
| Compose project | `hmdp-aiops-demo` |
| MySQL | `127.0.0.1:13307`，volume `hmdp-aiops-demo-mysql-data` |
| Redis | `127.0.0.1:16379`，volume `hmdp-aiops-demo-redis-data` |
| Kafka | `127.0.0.1:29092`，volume `hmdp-aiops-demo-kafka-data` |
| Main topic | `hmdp.aiops.mysql-demo.order.main.v1` |
| Retry topic | `hmdp.aiops.mysql-demo.order.retry.v1` |
| DLT topic | `hmdp.aiops.mysql-demo.order.dlt.v1` |
| Main group | `hmdp-aiops-mysql-demo-consumer-v1` |
| Retry group | `hmdp-aiops-mysql-demo-retry-v1` |
| DLT group | `hmdp-aiops-mysql-demo-dlt-v1` |

实测 `netstat` 中两个 JVM 的依赖连接只出现 `13307`、`16379` 和 `29092`，未出现原 MySQL `3306/3307`。Kafka Demo Broker 的业务 Topic 列表也仅包含上表三条 `hmdp.aiops.mysql-demo.*` Topic。

## 3. Profile 责任

### Web

组合 Profile：`web,web-aiops-demo`。

- 保留 Controller、秒杀 HTTP 入口和 Kafka Producer。
- `hmdp.kafka.listener.enabled=false`，不创建订单 Kafka Listener。
- `hmdp.seckill.redis-stream-consumer.enabled=false`，使用 Kafka 消息链路。
- Redis、Kafka、MySQL 分别指向 `16379`、`29092`、`13307`。

### Consumer

组合 Profile：`consumer,consumer-aiops-demo`。

- `spring.main.web-application-type=none`，不暴露业务 Controller。
- `hmdp.kafka.listener.enabled=true`，启动 main、retry、DLT Listener。
- 开启 loopback-only MySQL health outlet：`127.0.0.1:18082/internal/aiops/mysql-health`。
- `spring.jackson.default-property-inclusion=always`，让不可用指标以显式 `null` 保留，满足 MySQL Health Tool 的严格 Observation Schema。
- Redis、Kafka、MySQL 分别指向 `16379`、`29092`、`13307`。

项目原 `RedissonConfig` 已改为读取 `spring.redis.host/port`，否则 Demo Profile 虽配置 16379，Redisson 仍会连接硬编码的原端口 6379。该调整只修复基础设施配置绑定，不改变订单逻辑或事务边界。

## 4. 启动命令

以下命令均在仓库根目录执行。

### 4.1 启动 Demo 基础设施

```powershell
& .\scripts\aiops-demo\start-demo.ps1
```

脚本只操作 `hmdp-aiops-demo` project，不删除 volume。

### 4.2 构建应用

```powershell
mvn.cmd test
mvn.cmd -DskipTests package
```

### 4.3 启动 Consumer JVM

```powershell
java --add-opens=java.base/java.lang.invoke=ALL-UNNAMED `
  -jar .\target\hm-dianping-0.0.1-SNAPSHOT.jar `
  --spring.profiles.active=consumer,consumer-aiops-demo
```

Java 17 必须保留 `java.lang.invoke` 的模块开放参数。项目使用的 MyBatis-Plus 3.4.3 会反射读取 `SerializedLambda`；缺少该参数会使消费进入 Retry/DLT，不能算作正常基线。

### 4.4 启动 Web JVM

```powershell
java -jar .\target\hm-dianping-0.0.1-SNAPSHOT.jar `
  --spring.profiles.active=web,web-aiops-demo
```

启动顺序必须为 Demo Docker、Consumer、Web。应先确认 Consumer Group 已有成员，再发送基线订单。

## 5. 正常业务链路实测

使用测试辅助脚本经过真实 HTTP 入口创建独立测试券并下单：

```powershell
& .\scripts\aiops-demo\Invoke-SeckillDemoOrders.ps1 `
  -Infrastructure aiops-demo `
  -Phase baseline_e20_success `
  -Phones @('19920923002') `
  -Stock 8 `
  -IUnderstandLocalTestData
```

实测返回：

```json
{
  "phase": "baseline_e20_success",
  "voucher_id": 11,
  "attempted": 1,
  "accepted": 1,
  "order_ids": [640664779597283330],
  "observed_at": "2026-09-23T11:06:33.2537360+08:00"
}
```

链路证据：

- Web 在 main topic partition 0、offset 1 成功发送 order `640664779597283330`。
- Consumer 在相同 Topic/partition/offset 成功消费并确认。
- Demo MySQL `hmdp.tb_voucher_order` 存在记录：`id=640664779597283330`、`user_id=1011`、`voucher_id=11`、`create_time=2026-09-23 11:06:33`。
- Demo 秒杀库存由 8 变为 7。
- main group `CURRENT-OFFSET=2`、`LOG-END-OFFSET=2`、`LAG=0`。

预检说明：启动参数校验阶段曾产生 order `640663997913235457`，当时 Consumer 缺少 Java 17 `java.lang.invoke` 开放参数，消息进入隔离 Retry/DLT，未落库。该记录不属于故障注入，也不计入成功基线；修正启动命令后使用新券、新用户和新订单完成了上述干净验证。

## 6. Agent 只读 Tool 验收

Agent 连接 Demo 环境时使用：

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:29092'
$env:KAFKA_VOUCHER_ORDER_TOPIC = 'hmdp.aiops.mysql-demo.order.main.v1'
$env:KAFKA_CONSUMER_GROUP = 'hmdp-aiops-mysql-demo-consumer-v1'
$env:AIOPS_MYSQL_HEALTH_PORT = '18082'
```

### `get_kafka_status`

使用重新打包后的 JAR、且不使用临时配置覆盖，于 2026-09-23 12:56:01 实测为 `success/complete`：

```json
{
  "evidence_id": "evi_18a64e11a9ca4aa4b4b05867531ea863",
  "status": "success",
  "kind": "kafka_consumer_status",
  "source_tool": "get_kafka_status",
  "summary": "Kafka group hmdp-aiops-mysql-demo-consumer-v1 state=stable, members=1, total_lag=0, lag_status=normal.",
  "data": {
    "topic": "hmdp.aiops.mysql-demo.order.main.v1",
    "consumer_group": "hmdp-aiops-mysql-demo-consumer-v1",
    "consumer_status": "stable",
    "member_count": 1,
    "total_lag": 0
  }
}
```

### `get_mysql_health`

使用重新打包后的 JAR、且不使用临时配置覆盖，于 2026-09-23 12:56:04 实测为 `partial`，但连接验证成功：

```json
{
  "evidence_id": "evi_34924b4cb8ef413f9ae83100ccdddcc1",
  "status": "partial",
  "kind": "mysql_health",
  "source_tool": "get_mysql_health",
  "summary": "Consumer MySQL connection validation succeeded.",
  "data": {
    "source_role": "hmdp-consumer",
    "database_reachable": true,
    "connection_test_status": "valid",
    "hikari_active": 0,
    "hikari_idle": 1,
    "connection_timeout_count": null,
    "error_count": null,
    "unavailable_metrics": ["connection_timeout_count", "error_count"]
  }
}
```

`partial` 是预期缺失语义：当前 Hikari MXBean 不提供累计 connection timeout/error count。Tool 没有伪造这两个指标。

### `get_business_metrics`

实测为 `partial`：固定白名单日志 `target/runtime/spring-boot.out.log` 与 `spring-boot.error.log` 可读，但当前正常运行日志没有五项显式业务指标 marker，因此五项数值为 0，并在 `unavailable_metrics` 中完整声明缺失。该 Observation 不能解释为“真实请求数为 0”，也不影响本阶段 Kafka/MySQL Tool 验收。Phase E2.0 未修改 Agent 或业务埋点来填补此缺口。

## 7. 验收结论

| 验收项 | 结果 | 证据 |
| --- | --- | --- |
| Demo MySQL 健康 | 通过 | `database_reachable=true`、`connection_test_status=valid` |
| Consumer 成功连接 | 通过 | Listener 分配 main/retry/DLT partition |
| Kafka Group member=1 | 通过 | Kafka Tool 返回 `member_count=1` |
| 发送订单成功 | 通过 | HTTP accepted=1，Producer main topic offset 1 |
| 订单落库成功 | 通过 | Demo MySQL order `640664779597283330` |
| Agent 读取 Demo Kafka/MySQL | 通过 | Kafka `success`；MySQL `partial` 且 reachable/valid |
| 不访问原 MySQL 3306/3307 | 通过 | JVM TCP 连接仅出现 Demo `13307` |

## 8. 停止顺序

先在各自终端用 `Ctrl+C` 停止 Web 和 Consumer JVM，再停止 Demo 依赖：

```powershell
& .\scripts\aiops-demo\stop-demo.ps1
```

停止脚本不执行 `down -v`，不会删除 Demo volume。
