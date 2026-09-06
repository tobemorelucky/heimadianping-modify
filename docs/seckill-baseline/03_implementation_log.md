# 秒杀基线实施日志

## 2026-09-06

### 实施内容

- 修复 Redis Stream ACK key，统一使用常量 `stream.orders/g1` 并校验 ACK 返回值。
- 将 Stream 消费从 `VoucherOrderServiceImpl` 拆到独立消费者组件。
- 新增独立事务 Service，确保数据库库存扣减与订单插入同事务。
- 为订单表增加 `(user_id, voucher_id)` 唯一索引及增量 migration。
- 新增持久化失败次数、3 次有限重试、500ms 退避和失败订单 Stream。
- 使用 Lua 原子完成“记录失败订单 + ACK 原消息 + 清理重试计数”。
- 自动创建 Stream 消费者组，消费者名支持环境变量配置。
- 增加秒杀准入、消费成功、重试、失败和锁释放异常日志。
- 增加消费线程优雅关闭。

### 修改文件

| 文件 | 原因 |
| --- | --- |
| `VoucherOrderServiceImpl.java` | 只保留 HTTP/Lua 准入职责，移除错误消费与无事务落库逻辑 |
| `RedisConstants.java` | 集中定义主 Stream、组、失败 Stream、重试 Hash 和最大次数 |
| `application.yaml` | 增加可配置的稳定消费者名 |
| `db/hmdp.sql` | 新安装数据库包含用户+优惠券唯一索引 |

### 新增文件

| 文件 | 作用 |
| --- | --- |
| `VoucherOrderStreamConsumer.java` | 正确 ACK、Pending 恢复、有限重试、失败记录、线程生命周期 |
| `VoucherOrderTransactionalService.java` | 数据库事务与幂等订单创建 |
| `seckill-record-failure.lua` | 原子记录失败消息并 ACK 原消息 |
| `db/migration/V20260906_01__add_voucher_order_unique_index.sql` | 已有数据库增量加唯一索引 |
| `VoucherOrderStreamConsumerTest.java` | 验证 ACK 使用正确 Stream 且 ACK=0 抛错 |
| `VoucherOrderTransactionalServiceTest.java` | 验证幂等、库存、插单和事务声明 |
| `SeckillDatabaseSchemaTest.java` | 验证唯一索引位于正确订单表 |

### 测试结果

专项命令：

```text
mvn -q -Dtest=VoucherOrderTransactionalServiceTest,VoucherOrderStreamConsumerTest,SeckillDatabaseSchemaTest test
```

结果：7 个测试全部通过，0 failure，0 error。

全量命令：

```text
mvn -q test
```

结果：共发现 13 个测试，8 个通过，5 个既有 Spring 集成测试报错。报错原因均为测试环境没有启动 `127.0.0.1:6379` Redis，Redisson 创建 Bean 时连接被拒绝；不是编译错误或新增专项测试失败。受影响的既有测试：

- `HmDianPingApplicationTests` 4 个。
- `RedissonTest` 1 个。

本次未伪造或跳过集成测试结果。具备 Redis/MySQL 的集成环境中仍需再次执行全量测试和真实 Stream 故障恢复测试。

### 实施中发现并修正

初次编辑全量 SQL 时，通用文本片段导致唯一索引短暂落入 `tb_follow` 定义。静态复核在交付前发现并精确修正到 `tb_voucher_order`，随后新增 `SeckillDatabaseSchemaTest` 防止回归；错误版本未作为最终结果交付。

### 当前状态

代码编译通过，专项测试通过；全量集成测试被本机 Redis 缺失阻塞。未引入 Kafka，HTTP 接口和 Lua 准入主流程未改变。

