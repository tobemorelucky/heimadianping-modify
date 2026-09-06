package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;

/**
 * 秒杀订单入口服务。
 *
 * <p>请求线程只负责生成订单号和执行 Redis Lua 准入，数据库落库由
 * {@link VoucherOrderStreamConsumer} 异步完成。</p>
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
        implements IVoucherOrderService {

    /** Redis 原子完成库存校验、一人一单校验、预扣库存和写入 Stream。 */
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

    /**
     * 执行秒杀准入并立即返回受理后的订单号。
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        // Lua 内部操作具备 Redis 原子性，成功后订单消息进入 stream.orders。
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
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

        log.info("秒杀请求已受理，voucherId={}, userId={}, orderId={}", voucherId, userId, orderId);
        return Result.ok(orderId);
    }
}
