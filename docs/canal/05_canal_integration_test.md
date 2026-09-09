# Canal Consumer 测试与联调记录

## 1. 测试范围

覆盖：

- INSERT、UPDATE、DELETE 的 id 提取。
- 非 `hmdp.tb_shop` 事件过滤。
- Redis key 存在和不存在时的幂等删除。
- Redis 结果未知时失败。
- 成功处理后 ACK。
- 处理失败 rollback 且不 ACK。
- 空 batch 不 ACK。
- 真实 MySQL UPDATE → Canal → Spring Consumer → Redis DEL。

## 2. 单元测试

执行：

```powershell
mvn "-Dtest=CanalClientTest,ShopBinlogEventHandlerTest,ShopCacheInvalidatorTest" test
```

结果：

```text
Tests run: 7, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

测试类：

- `CanalClientTest`：成功 ACK、异常 rollback、不空提交。
- `ShopBinlogEventHandlerTest`：INSERT/UPDATE/DELETE 与库表过滤。
- `ShopCacheInvalidatorTest`：Redis 删除结果与幂等语义。

## 3. 真实集成测试

测试类 `CanalShopCacheIntegrationTest` 默认跳过，避免普通 `mvn test` 修改本地数据库。只有显式设置环境变量才运行：

```powershell
$env:CANAL_INTEGRATION_TEST = 'true'
mvn "-Dtest=CanalShopCacheIntegrationTest" test
```

测试步骤：

1. 等待 Spring Canal Client 已连接。
2. 保存 `tb_shop.id=1` 的原名称。
3. 写入 `cache:shop:1` 测试值。
4. 执行 `UPDATE tb_shop SET name='test' WHERE id=1`。
5. 最长等待 15 秒，确认 `cache:shop:1` 被删除。
6. finally 恢复原店名并清理缓存。

实测关键日志：

```text
Canal consumer connected, host=127.0.0.1:11111, destination=hmdp
Canal shop cache invalidated, shopId=1, key=cache:shop:1, existed=true
Canal shop binlog batch handled, shopIds=[1]
Canal batch acknowledged
```

验证结果：

- 集成测试：1 个通过，0 失败。
- Redis `EXISTS cache:shop:1`：`0`。
- MySQL 店铺名称：已恢复，不是测试值 `test`。
- Canal Server 日志：收到 Client 订阅并应用 `^hmdp\.tb_shop$` 过滤器。

首次联调曾因订阅正则多一层转义而无法匹配 UPDATE。日志暴露为 `hmdp\\.tb_shop`；修正为实际值 `hmdp\.tb_shop` 后重跑通过。失败轮次同样通过 finally 恢复了数据。

## 4. 完整回归

执行：

```powershell
mvn "-Dcanal.enabled=false" test
```

完整回归关闭 Canal 自动连接，真实链路由上一节独立测试负责，避免普通回归消费本地 binlog。

结果：

```text
Tests run: 43, Failures: 0, Errors: 0, Skipped: 3
BUILD SUCCESS
```

3 个跳过项是显式环境门控的 Canal、Kafka 和 Elasticsearch 真实环境测试。

## 5. 环境说明

- Canal：`hmdp-canal`，运行状态 `healthy`。
- MySQL：本机 3306。
- 本机原 Redis 未运行，因此联调使用已有镜像 `redis:7-alpine` 启动无持久化、带 `--rm` 的临时容器。
- 两轮测试结束后临时 Redis 均已停止并自动删除。
- Kafka 9092 当时未运行，产生的连接告警与 Canal 测试无关，未改动 Kafka 代码。

## 6. 手工复测

```text
SET cache:shop:1 <任意测试值>
UPDATE hmdp.tb_shop SET name='test' WHERE id=1;
查看应用日志中的 ShopCacheInvalidator
EXISTS cache:shop:1
```

手工修改业务数据前必须保存原值并在验证后恢复。
