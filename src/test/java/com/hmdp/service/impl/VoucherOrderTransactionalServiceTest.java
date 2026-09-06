package com.hmdp.service.impl;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.SeckillVoucherMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.annotation.Transactional;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 验证秒杀订单数据库事务服务的幂等、库存和插单边界。
 */
@ExtendWith(MockitoExtension.class)
class VoucherOrderTransactionalServiceTest {

    @Mock
    private VoucherOrderMapper voucherOrderMapper;

    @Mock
    private SeckillVoucherMapper seckillVoucherMapper;

    private VoucherOrderTransactionalService service;

    /** 每个用例使用独立的事务服务实例。 */
    @BeforeEach
    void setUp() {
        service = new VoucherOrderTransactionalService(voucherOrderMapper, seckillVoucherMapper);
    }

    /** 已存在的业务订单应直接按幂等成功处理，不能再次扣库存。 */
    @Test
    void shouldTreatExistingOrderAsIdempotentSuccess() {
        VoucherOrder order = order();
        when(voucherOrderMapper.selectCount(any())).thenReturn(1);

        service.createVoucherOrder(order);

        verify(seckillVoucherMapper, never()).update(isNull(), any());
        verify(voucherOrderMapper, never()).insert(any());
    }

    /** 库存扣减成功后应插入同一条订单。 */
    @Test
    void shouldDecreaseStockAndInsertOrder() {
        VoucherOrder order = order();
        when(voucherOrderMapper.selectCount(any())).thenReturn(0);
        when(seckillVoucherMapper.update(isNull(), any())).thenReturn(1);
        when(voucherOrderMapper.insert(order)).thenReturn(1);

        service.createVoucherOrder(order);

        verify(seckillVoucherMapper).update(isNull(), any());
        verify(voucherOrderMapper).insert(order);
    }

    /** 数据库库存不足时必须抛出异常，不能继续创建订单。 */
    @Test
    void shouldRejectOrderWhenDatabaseStockCannotBeDecreased() {
        VoucherOrder order = order();
        when(voucherOrderMapper.selectCount(any())).thenReturn(0);
        when(seckillVoucherMapper.update(isNull(), any())).thenReturn(0);

        assertThrows(IllegalStateException.class, () -> service.createVoucherOrder(order));

        verify(voucherOrderMapper, never()).insert(any());
    }

    /** 事务注解必须保留在对外方法上，确保由 Spring AOP 建立事务边界。 */
    @Test
    void shouldDeclareTransactionBoundary() throws NoSuchMethodException {
        Transactional transactional = VoucherOrderTransactionalService.class
                .getMethod("createVoucherOrder", VoucherOrder.class)
                .getAnnotation(Transactional.class);

        assertNotNull(transactional);
    }

    /** 构造字段完整的秒杀订单消息。 */
    private VoucherOrder order() {
        return new VoucherOrder()
                .setId(1001L)
                .setUserId(2001L)
                .setVoucherId(3001L);
    }
}
