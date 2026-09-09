package com.hmdp.config.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

/**
 * 等待 Retry/DLT 转发结果的失败消息恢复器。
 *
 * <p>Spring Kafka 2.5 的默认恢复器异步发送后立即返回；本实现只有收到 broker 成功结果
 * 才让错误处理器把源记录视为已恢复，避免转发失败时错误提交源 offset。</p>
 */
public class ReliableDeadLetterPublishingRecoverer extends DeadLetterPublishingRecoverer {

    /** 失败消息转发等待上限，避免 broker 异常时无限阻塞消费线程。 */
    private static final long SEND_TIMEOUT_SECONDS = 10L;

    /** 使用指定目标解析器创建可靠转发恢复器。 */
    public ReliableDeadLetterPublishingRecoverer(
            KafkaOperations<? extends Object, ? extends Object> template,
            BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver) {
        super(template, destinationResolver);
    }

    /**
     * 同步等待失败消息获得 broker 确认；失败或超时则抛出异常，禁止提交源 offset。
     */
    @Override
    protected void publish(
            ProducerRecord<Object, Object> outRecord,
            KafkaOperations<Object, Object> template) {
        try {
            SendResult<Object, Object> result = template.send(outRecord)
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            logger.debug(() -> "Failed record forwarded successfully: " + result);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("转发 Kafka 失败消息时线程被中断", exception);
        } catch (ExecutionException | TimeoutException exception) {
            throw new IllegalStateException("转发 Kafka 失败消息失败", exception);
        }
    }
}
