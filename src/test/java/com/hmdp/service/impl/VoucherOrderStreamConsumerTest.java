package com.hmdp.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_STREAM_GROUP;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_STREAM_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 验证订单消费者只能确认真实的订单 Stream。
 */
@ExtendWith(MockitoExtension.class)
class VoucherOrderStreamConsumerTest {

    @Mock
    private StringRedisTemplate stringRedisTemplate;

    @Mock
    private StreamOperations<String, Object, Object> streamOperations;

    @Mock
    private RedissonClient redissonClient;

    @Mock
    private VoucherOrderTransactionalService transactionalService;

    private VoucherOrderStreamConsumer consumer;

    /** 每个用例构造一个未启动后台线程的消费者。 */
    @BeforeEach
    void setUp() {
        when(stringRedisTemplate.opsForStream()).thenReturn(streamOperations);
        consumer = new VoucherOrderStreamConsumer(stringRedisTemplate, redissonClient, transactionalService);
    }

    /** ACK 必须使用 stream.orders 和 g1，不能再使用旧的错误 key s1。 */
    @Test
    void shouldAcknowledgeTheActualOrderStream() {
        RecordId recordId = RecordId.of("1-0");
        when(streamOperations.acknowledge(
                SECKILL_ORDER_STREAM_KEY, SECKILL_ORDER_STREAM_GROUP, recordId)).thenReturn(1L);

        consumer.acknowledge(recordId);

        verify(streamOperations).acknowledge(
                SECKILL_ORDER_STREAM_KEY, SECKILL_ORDER_STREAM_GROUP, recordId);
    }

    /** Redis 返回 0 表示消息未被确认，消费者必须将其视为失败。 */
    @Test
    void shouldFailWhenRedisDoesNotAcknowledgeTheRecord() {
        RecordId recordId = RecordId.of("1-0");
        when(streamOperations.acknowledge(
                SECKILL_ORDER_STREAM_KEY, SECKILL_ORDER_STREAM_GROUP, recordId)).thenReturn(0L);

        assertThrows(IllegalStateException.class, () -> consumer.acknowledge(recordId));
    }
}
