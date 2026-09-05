# PROJECT ANALYSIS SUMMARY

一句话总结当前项目：当前 `hm-dianping 0.0.1-SNAPSHOT` 是基于 Spring Boot 2.3.12、Java 8、MySQL 与 Redis 的单体点评系统，已具备 Redis 缓存、GEO、社交数据结构以及 Lua + Redis Stream 异步秒杀雏形，但尚未达到生产级消息可靠性和数据一致性要求。

## 当前版本

`com.hmdp:hm-dianping:0.0.1-SNAPSHOT`；Spring Boot `2.3.12.RELEASE`；Java `8`；MySQL 脚本基线 `5.6.22`；Spring Data Redis `2.6.2`；Lettuce `6.1.6.RELEASE`；Redisson `3.13.6`。

## 后续升级方向

先修复秒杀 ACK、事务、幂等、初始化和安全配置，再以 Redis Stream Outbox + Kafka 建立可靠秒杀事件链路；通过 Canal CDC 统一驱动 Redis 缓存/GEO 失效与 Elasticsearch 店铺搜索索引；最后以独立、只读优先、全审计和人工审批的方式接入 AI 运营助手。

## 风险

最高风险是秒杀消费者对错误 Stream key `s1` ACK、库存扣减和订单写入不在同一事务、订单缺少业务唯一约束，以及失败分支可能确认并丢弃已预扣的请求；其次是登录与管理接口安全边界薄弱、缓存无法感知外部写库、ES/Canal/Kafka 尚不存在、Redis 永久 key 与人工初始化缺少治理。

## 文档索引

- [项目基线](00_project_baseline.md)
- [架构与代码结构](01_architecture_analysis.md)
- [数据库分析](02_database_analysis.md)
- [Redis 使用分析](03_redis_usage_analysis.md)
- [秒杀当前流程](04_seckill_current_flow.md)
- [未来升级规划](05_future_upgrade_plan.md)
- [开发日志](development_log.md)

