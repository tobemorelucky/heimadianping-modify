# Canal Consumer 实现说明

## 1. 实现目标

监听 Canal Server 中 `hmdp.tb_shop` 的 INSERT、UPDATE、DELETE 行事件，解析店铺 id，并删除：

```text
cache:shop:{shopId}
```

本阶段使用 Canal 原生 Client，不引入 Canal Adapter，不修改 Kafka、Elasticsearch、Lua 或秒杀链路。

## 2. 最终结构

```text
com.hmdp.canal
├── config
│   ├── CanalConfig
│   └── CanalProperties
├── client
│   └── CanalClient
├── handler
│   └── ShopBinlogEventHandler
└── service
    └── ShopCacheInvalidator
```

新增 Maven 依赖：

- `com.alibaba.otter:canal.client:1.1.8`
- `com.alibaba.otter:canal.protocol:1.1.8`

`canal.protocol` 需要显式声明，因为 `canal.client` 将其标记为 optional，而业务代码需要直接解析 `CanalEntry`。

## 3. 消费流程

```text
CanalClient 启动
    ↓
连接 127.0.0.1:11111 / destination=hmdp
    ↓
subscribe hmdp\.tb_shop
    ↓
getWithoutAck(batch)
    ↓
ShopBinlogEventHandler
    ├─ 校验 database=hmdp
    ├─ 校验 table=tb_shop
    ├─ INSERT：afterColumns 取 id
    ├─ UPDATE：afterColumns 取 id
    └─ DELETE：beforeColumns 取 id
    ↓
同 batch shopId 去重
    ↓
ShopCacheInvalidator
    ↓
DEL cache:shop:{id}
    ├─ 成功或 key 已不存在：ack(batchId)
    └─ 异常/结果未知：rollback(batchId)，断开后退避重连
```

## 4. ACK 边界

`CanalClient` 使用 `getWithoutAck`，没有提前确认：

1. 拉取非空 batch。
2. 完成 Entry 解析。
3. 完成该 batch 中全部目标店铺缓存删除。
4. 最后调用 `ack(batchId)`。

解析异常、缺少 id、非法 id 或 Redis 删除异常都会调用 `rollback(batchId)`，随后由外层连接循环重连并重放。

空消息 `batchId=-1` 不 ACK，避免空提交。

## 5. Redis 删除语义

`StringRedisTemplate.delete(key)`：

- 返回 `true`：key 存在且已删除，成功。
- 返回 `false`：key 原本不存在，DEL 已确定执行，按幂等成功。
- 返回 `null` 或抛异常：结果不确定，抛出异常阻止 ACK。

同一 binlog 重复交付不会产生重复数据，Redis DEL 天然幂等。

## 6. 配置管理

`application.yaml` 新增：

```yaml
canal:
  enabled: ${CANAL_ENABLED:true}
  host: ${CANAL_HOST:127.0.0.1}
  port: ${CANAL_PORT:11111}
  destination: ${CANAL_DESTINATION:hmdp}
  subscription: ${CANAL_SUBSCRIPTION:hmdp\.tb_shop}
```

此外提供 Client 认证、batch size、poll timeout 和重连间隔环境变量。连接地址、端口、destination 和订阅均未写死在 Java 代码中。

## 7. 生命周期与异常处理

- 使用 Spring `SmartLifecycle` 自动启动和停止。
- 单独 daemon 线程消费，不阻塞应用启动。
- 无数据时由 Canal timeout poll 等待，避免忙轮询。
- Canal/MySQL/网络异常时记录错误，固定退避后重连。
- 重连后先执行无参 `rollback()`，使上次未确认 batch 可再次投递。
- 应用关闭时中断重连等待，由消费线程 finally 断开 Connector。

## 8. 修改文件

新增：

- `src/main/java/com/hmdp/canal/config/CanalConfig.java`
- `src/main/java/com/hmdp/canal/config/CanalProperties.java`
- `src/main/java/com/hmdp/canal/client/CanalClient.java`
- `src/main/java/com/hmdp/canal/handler/ShopBinlogEventHandler.java`
- `src/main/java/com/hmdp/canal/service/ShopCacheInvalidator.java`
- 三个 Canal 单元测试与一个独立真实环境测试。

修改：

- `pom.xml`：增加 Canal Client/Protocol。
- `src/main/resources/application.yaml`：增加 `canal.*` 配置。

未修改 Kafka、ES、Lua、秒杀、ShopController、ShopService 或 Redis 原有业务逻辑。

## 9. 官方实现依据

Canal 官方 Client 示例使用 `newSingleConnector`、`subscribe`、`getWithoutAck`、成功 `ack` 和失败 `rollback` 的消费模式：

https://github.com/alibaba/canal/wiki/ClientExample
