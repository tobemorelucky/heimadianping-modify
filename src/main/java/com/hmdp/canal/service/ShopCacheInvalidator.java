package com.hmdp.canal.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;

/**
 * 统一删除店铺详情缓存。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShopCacheInvalidator {

    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 删除指定店铺缓存。
     *
     * <p>返回 false 仅表示 key 原本不存在，DEL 命令本身仍执行成功，
     * 因而按幂等成功处理；异常或 Redis 返回未知状态时阻止 Canal ACK。</p>
     */
    public void invalidate(Long shopId) {
        if (shopId == null || shopId <= 0) {
            throw new IllegalArgumentException("shopId must be positive");
        }
        String key = CACHE_SHOP_KEY + shopId;
        Boolean deleted = stringRedisTemplate.delete(key);
        if (deleted == null) {
            throw new IllegalStateException("Redis did not return deletion result for " + key);
        }
        log.info("Canal shop cache invalidated, shopId={}, key={}, existed={}",
                shopId, key, deleted);
    }
}
