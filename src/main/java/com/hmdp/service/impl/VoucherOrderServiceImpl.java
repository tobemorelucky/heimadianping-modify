package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.kafka.message.VoucherOrderMessage;
import com.hmdp.kafka.producer.VoucherOrderKafkaProducer;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 秒杀订单入口服务。
 *
 * <p>请求线程负责生成订单号、执行 Redis Lua 准入并按配置投递订单消息，
 * 数据库落库由 Redis Stream 或 Kafka 消费者异步完成。</p>
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    private static final String MESSAGE_MODE_REDIS = "redis";
    private static final String MESSAGE_MODE_KAFKA = "kafka";
    private static final long KAFKA_SEND_TIMEOUT_SECONDS = 10L;

    /** Redis 原子完成库存校验、一人一单校验、预扣库存和按模式写入 Stream。 */
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private VoucherOrderKafkaProducer kafkaProducer;

    /** 秒杀消息通道开关，默认使用 Kafka，保留 redis 值用于灰度回退。 */
    @Value("${seckill.message.mode:kafka}")
    private String messageMode;

    /**
     * 执行秒杀准入并立即返回受理后的订单号。
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        String currentMessageMode = normalizeMessageMode();
        if (currentMessageMode == null) {
            log.error("秒杀消息模式配置非法, messageMode={}", messageMode);
            return Result.fail("秒杀服务配置错误");
        }

        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        // Lua 内部原子完成库存校验、预扣库存和一人一单标记；redis 模式同时写入 Stream。
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId), currentMessageMode
        );
        if (result == null) {
            log.error("秒杀脚本未返回结果，voucherId={}, userId={}, orderId={}", voucherId, userId, orderId);
            return Result.fail("秒杀服务繁忙，请稍后重试");
        }

        int code = result.intValue();
        if (code != 0) {
            log.info("秒杀资格校验失败，voucherId={}, userId={}, resultCode={}", voucherId, userId, code);
            return Result.fail(code == 1 ? "库存不足" : "不能重复下单");
        }

        if (MESSAGE_MODE_KAFKA.equals(currentMessageMode)) {
            VoucherOrderMessage message = new VoucherOrderMessage(
                    orderId, userId, voucherId, LocalDateTime.now());
            try {
                // 等待 broker 确认，避免请求已返回成功但消息仍停留在客户端发送队列。
                kafkaProducer.sendOrderMessage(message)
                        .get(KAFKA_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                log.error("Kafka秒杀订单消息发送被中断, orderId={}, userId={}, voucherId={}",
                        orderId, userId, voucherId, exception);
                return Result.fail("秒杀服务繁忙，请稍后重试");
            } catch (ExecutionException | TimeoutException | RuntimeException exception) {
                log.error("Kafka秒杀订单消息发送失败, orderId={}, userId={}, voucherId={}",
                        orderId, userId, voucherId, exception);
                return Result.fail("秒杀服务繁忙，请稍后重试");
            }
        }

        log.info("秒杀请求已受理，voucherId={}, userId={}, orderId={}, messageMode={}",
                voucherId, userId, orderId, currentMessageMode);
        return Result.ok(orderId);
    }

    /** 规范化并校验消息模式，防止配置错误导致 Lua 扣减后没有消息可消费。 */
    private String normalizeMessageMode() {
        if (messageMode == null) {
            return null;
        }
        String normalized = messageMode.trim().toLowerCase(Locale.ROOT);
        return MESSAGE_MODE_REDIS.equals(normalized) || MESSAGE_MODE_KAFKA.equals(normalized)
                ? normalized
                : null;
    }
}
