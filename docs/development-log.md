# 开发日志

## 2026-09-06

### 日期

2026-09-06

### 修改文件

- `src/main/java/com/hmdp/service/impl/VoucherOrderServiceImpl.java`
- `src/main/java/com/hmdp/utils/RedisConstants.java`
- `src/main/resources/application.yaml`
- `src/main/resources/db/hmdp.sql`

### 新增文件

- `src/main/java/com/hmdp/service/impl/VoucherOrderStreamConsumer.java`
- `src/main/java/com/hmdp/service/impl/VoucherOrderTransactionalService.java`
- `src/main/resources/seckill-record-failure.lua`
- `src/main/resources/db/migration/V20260906_01__add_voucher_order_unique_index.sql`
- 3 个秒杀专项测试类
- `docs/seckill-baseline/` 下 4 份专项文档
- `SECKILL_BASELINE_RESULT.md`

### 修改原因

修复 Redis Stream 错误 ACK、无有效数据库事务、缺少一人一券唯一约束、无限失败重试和失败订单不可追踪问题；保持现有 Redis Stream 秒杀架构，不引入 Kafka。

### 测试结果

- 秒杀专项测试：7/7 通过。
- 全量测试：13 个中 8 个通过，5 个既有 Spring 集成测试因本机 Redis `127.0.0.1:6379` 未启动而报错。
- Maven 编译成功；未发现新增逻辑的编译错误。

### 当前状态

秒杀基线加固实现完成。上线前需清理潜在重复订单并执行唯一索引 migration；具备 MySQL/Redis 的环境需补跑全量集成与故障恢复测试。

