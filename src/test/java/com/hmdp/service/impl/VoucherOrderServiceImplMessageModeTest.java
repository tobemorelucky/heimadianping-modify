package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.kafka.message.VoucherOrderMessage;
import com.hmdp.kafka.producer.VoucherOrderKafkaProducer;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** 验证秒杀入口的 Kafka 默认链路与 Redis Stream 灰度回退行为。 */
@ExtendWith(MockitoExtension.class)
class VoucherOrderServiceImplMessageModeTest {

    private static final long ORDER_ID = 1001L;
    private static final long USER_ID = 2001L;
    private static final long VOUCHER_ID = 3001L;

    @Mock
    private RedisIdWorker redisIdWorker;

    @Mock
    private StringRedisTemplate stringRedisTemplate;

    @Mock
    private VoucherOrderKafkaProducer kafkaProducer;

    private VoucherOrderServiceImpl service;

    /** 初始化独立服务对象和当前请求用户，不加载 Spring 上下文。 */
    @BeforeEach
    void setUp() {
        service = new VoucherOrderServiceImpl();
        ReflectionTestUtils.setField(service, "redisIdWorker", redisIdWorker);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", stringRedisTemplate);
        ReflectionTestUtils.setField(service, "kafkaProducer", kafkaProducer);

        UserDTO user = new UserDTO();
        user.setId(USER_ID);
        UserHolder.saveUser(user);
        when(redisIdWorker.nextId("order")).thenReturn(ORDER_ID);
    }

    /** 清理线程变量，避免影响同一测试进程中的其他用例。 */
    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    /** Kafka 模式下 Lua 只做准入，成功后必须等待 Kafka Producer 完成发送。 */
    @Test
    void shouldSendKafkaMessageWhenModeIsKafka() {
        ReflectionTestUtils.setField(service, "messageMode", "kafka");
        stubLuaSuccess();
        SettableListenableFuture<SendResult<String, VoucherOrderMessage>> completedFuture =
                new SettableListenableFuture<>();
        completedFuture.set(null);
        when(kafkaProducer.sendOrderMessage(any(VoucherOrderMessage.class)))
                .thenReturn(completedFuture);

        Result result = service.seckillVoucher(VOUCHER_ID);

        assertTrue(result.getSuccess());
        assertEquals(ORDER_ID, result.getData());
        ArgumentCaptor<VoucherOrderMessage> messageCaptor =
                ArgumentCaptor.forClass(VoucherOrderMessage.class);
        verify(kafkaProducer).sendOrderMessage(messageCaptor.capture());
        assertEquals(ORDER_ID, messageCaptor.getValue().getOrderId());
        assertEquals(USER_ID, messageCaptor.getValue().getUserId());
        assertEquals(VOUCHER_ID, messageCaptor.getValue().getVoucherId());
        verifyLuaMode("kafka");
    }

    /** Redis 模式下 Lua 继续写 Stream，入口不得额外发送 Kafka 消息。 */
    @Test
    void shouldRetainRedisStreamPathWhenModeIsRedis() {
        ReflectionTestUtils.setField(service, "messageMode", "redis");
        stubLuaSuccess();

        Result result = service.seckillVoucher(VOUCHER_ID);

        assertTrue(result.getSuccess());
        assertEquals(ORDER_ID, result.getData());
        verify(kafkaProducer, never()).sendOrderMessage(any());
        verifyLuaMode("redis");
    }

    /** 模拟 Lua 完成库存、一人一单校验与 Redis 预扣。 */
    @SuppressWarnings("unchecked")
    private void stubLuaSuccess() {
        doReturn(0L).when(stringRedisTemplate).execute(
                any(DefaultRedisScript.class),
                eq(Collections.emptyList()),
                any(), any(), any(), any());
    }

    /** 验证消息模式确实作为 Lua 第四个参数传入，避免双写或无消息。 */
    @SuppressWarnings("unchecked")
    private void verifyLuaMode(String expectedMode) {
        verify(stringRedisTemplate).execute(
                any(DefaultRedisScript.class),
                eq(Collections.emptyList()),
                eq(String.valueOf(VOUCHER_ID)),
                eq(String.valueOf(USER_ID)),
                eq(String.valueOf(ORDER_ID)),
                eq(expectedMode));
    }
}
