package com.hmdp.canal.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 店铺缓存删除服务单元测试。
 */
class ShopCacheInvalidatorTest {

    private StringRedisTemplate redisTemplate;
    private ShopCacheInvalidator invalidator;

    @BeforeEach
    void setUp() {
        redisTemplate = mock(StringRedisTemplate.class);
        invalidator = new ShopCacheInvalidator(redisTemplate);
    }

    /**
     * key 存在和不存在都表示 Redis DEL 已确定执行，均按幂等成功处理。
     */
    @Test
    void shouldTreatExistingAndMissingKeyAsSuccessfulInvalidation() {
        String key = CACHE_SHOP_KEY + 1L;
        when(redisTemplate.delete(key)).thenReturn(true, false);

        assertDoesNotThrow(() -> invalidator.invalidate(1L));
        assertDoesNotThrow(() -> invalidator.invalidate(1L));

        verify(redisTemplate, org.mockito.Mockito.times(2)).delete(key);
    }

    /**
     * Redis 未返回执行结果时必须失败，使 Canal batch 不会被 ACK。
     */
    @Test
    void shouldFailWhenRedisDeletionResultIsUnknown() {
        when(redisTemplate.delete(CACHE_SHOP_KEY + 2L)).thenReturn(null);

        assertThrows(IllegalStateException.class, () -> invalidator.invalidate(2L));
    }
}
