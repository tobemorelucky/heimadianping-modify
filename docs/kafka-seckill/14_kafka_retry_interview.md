# Kafka Retry/DLT 面试要点

## 为什么需要 Retry Topic？

Retry Topic 把故障消息从主消费分区移开。若数据库短时不可用，主 Topic 不必围绕同一条消息持续重试，后续订单仍可推进；失败订单在独立消费组中按有限策略再次处理。它同时把正常流量和故障流量隔离，便于分别监控 lag、扩容和限速。

## 为什么不用无限重试？

Kafka 分区内按顺序消费。一条永久失败的“毒消息”无限重试，会永久阻塞该分区，持续占用线程和数据库连接，并制造大量重复日志。无限重试没有失败终态，也无法区分短暂故障与数据错误。有限重试允许短暂故障自行恢复，超过阈值则转 DLT，主链路继续服务。

## DLT 如何处理？

DLT 是失败终态，不是垃圾桶。消费者至少记录订单标识、用户、优惠券、原 Topic/partition/offset、异常原因和时间，并建立告警。运维先判断订单是否已经落库、Redis 是否预扣、失败是否可恢复；修复根因后再按 `userId` key 受控重放。不能无条件自动重放，否则会再次形成故障风暴。

## 如何避免失败转发时丢消息？

监听方法在异常路径不 ACK。恢复器将原消息转发到 Retry 或 DLT，并等待 broker 确认；只有转发成功后，错误处理器才提交源 offset。发送失败、超时或线程中断都会抛错并保留源 offset。消费和转发仍是至少一次语义，因此数据库必须保留订单主键与 `(user_id, voucher_id)` 唯一索引。

## FixedBackOff 的参数如何理解？

当前 Retry Topic 配置 `FixedBackOff(1000ms, 1)`：Retry Topic 第一次消费失败后等待 1 秒，再执行一次；第二次仍失败时恢复器将记录转发 DLT。主 Topic 配置 `FixedBackOff(0, 0)`，表示主链路不做原地重试，第一次失败立即隔离到 Retry Topic。

## 为什么代码没有直接使用 DefaultErrorHandler？

当前项目是 Spring Boot 2.3.12、Spring Kafka 2.5.14，该版本没有 `DefaultErrorHandler`。因此使用当时对应的 `SeekToCurrentErrorHandler + FixedBackOff + DeadLetterPublishingRecoverer` 实现同样的有限重试与恢复语义。将来整体升级 Boot/Spring Kafka 后，可将错误处理器一对一迁移为 `DefaultErrorHandler`，无需改变 Topic 和业务消费逻辑。

