package com.hmdp.mapper;

import com.hmdp.entity.VoucherOrder;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.dto.AiCouponStatisticsDTO;
import com.hmdp.dto.AiOrderStatisticsDTO;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.time.LocalDateTime;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface VoucherOrderMapper extends BaseMapper<VoucherOrder> {

    /** 在 MySQL 内聚合店铺指定时间窗口内的有效订单。 */
    @Select("SELECT COUNT(*) AS orderCount, "
            + "ROUND(COALESCE(SUM(v.pay_value), 0) / 100, 2) AS transactionAmount "
            + "FROM tb_voucher_order vo "
            + "INNER JOIN tb_voucher v ON v.id = vo.voucher_id "
            + "WHERE v.shop_id = #{shopId} "
            + "AND vo.create_time >= #{startTime} "
            + "AND vo.create_time < #{endTime} "
            + "AND vo.status NOT IN (4, 5, 6)")
    AiOrderStatisticsDTO queryOrderStatistics(
            @Param("shopId") Long shopId,
            @Param("startTime") LocalDateTime startTime,
            @Param("endTime") LocalDateTime endTime
    );

    /** 在 MySQL 内聚合店铺优惠券的发放量与核销量。 */
    @Select("SELECT COUNT(*) AS issuedCount, "
            + "COALESCE(SUM(CASE WHEN vo.status = 3 OR vo.use_time IS NOT NULL "
            + "THEN 1 ELSE 0 END), 0) AS usedCount "
            + "FROM tb_voucher_order vo "
            + "INNER JOIN tb_voucher v ON v.id = vo.voucher_id "
            + "WHERE v.shop_id = #{shopId}")
    AiCouponStatisticsDTO queryCouponStatistics(@Param("shopId") Long shopId);
}
