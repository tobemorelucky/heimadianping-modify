package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 秒杀订单数据库事务边界。
 *
 * <p>该类独立于 Stream 消费者，确保方法经由 Spring 代理调用，库存扣减和
 * 订单插入要么同时提交，要么同时回滚。</p>
 */
@Slf4j
@Service
public class VoucherOrderTransactionalService {

    private final VoucherOrderMapper voucherOrderMapper;
    private final SeckillVoucherMapper seckillVoucherMapper;

    /**
     * 构造数据库事务服务。
     */
    public VoucherOrderTransactionalService(VoucherOrderMapper voucherOrderMapper,
                                            SeckillVoucherMapper seckillVoucherMapper) {
        this.voucherOrderMapper = voucherOrderMapper;
        this.seckillVoucherMapper = seckillVoucherMapper;
    }

    /**
     * 幂等创建订单，并在同一事务中扣减数据库库存。
     *
     * @param voucherOrder Lua 产生的订单消息
     */
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        validateOrder(voucherOrder);

        // 消息可能重复投递；已存在的业务订单按幂等成功处理。
        Integer existingOrders = voucherOrderMapper.selectCount(
                new LambdaQueryWrapper<VoucherOrder>()
                        .eq(VoucherOrder::getUserId, voucherOrder.getUserId())
                        .eq(VoucherOrder::getVoucherId, voucherOrder.getVoucherId())
        );
        if (existingOrders != null && existingOrders > 0) {
            log.info("秒杀订单已存在，按幂等成功处理，userId={}, voucherId={}, orderId={}",
                    voucherOrder.getUserId(), voucherOrder.getVoucherId(), voucherOrder.getId());
            return;
        }

        // 条件更新是数据库库存不超卖的最终防线。
        int updatedRows = seckillVoucherMapper.update(
                null,
                new LambdaUpdateWrapper<SeckillVoucher>()
                        .setSql("stock = stock - 1")
                        .eq(SeckillVoucher::getVoucherId, voucherOrder.getVoucherId())
                        .gt(SeckillVoucher::getStock, 0)
        );
        if (updatedRows != 1) {
            throw new IllegalStateException("数据库秒杀库存不足或优惠券不存在");
        }

        // 插入失败会抛出异常并触发整个事务回滚，包括前面的库存扣减。
        int insertedRows = voucherOrderMapper.insert(voucherOrder);
        if (insertedRows != 1) {
            throw new IllegalStateException("秒杀订单创建失败");
        }
    }

    /**
     * 拒绝字段不完整的消息，避免错误数据进入数据库。
     */
    private void validateOrder(VoucherOrder voucherOrder) {
        if (voucherOrder == null
                || voucherOrder.getId() == null
                || voucherOrder.getUserId() == null
                || voucherOrder.getVoucherId() == null) {
            throw new IllegalArgumentException("秒杀订单消息字段不完整");
        }
    }
}
