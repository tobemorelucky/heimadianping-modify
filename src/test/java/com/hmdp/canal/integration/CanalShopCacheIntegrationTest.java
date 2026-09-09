package com.hmdp.canal.integration;

import com.hmdp.canal.client.CanalClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 独立真实环境测试：MySQL UPDATE -> Canal -> Spring Consumer -> Redis DEL。
 *
 * <p>默认跳过，只有显式设置 CANAL_INTEGRATION_TEST=true 才修改本地测试数据，
 * 并在 finally 中恢复店铺原名称和清理测试缓存。</p>
 */
@SpringBootTest(properties = {
        "canal.enabled=true",
        "spring.kafka.listener.auto-startup=false"
})
@EnabledIfEnvironmentVariable(named = "CANAL_INTEGRATION_TEST", matches = "true")
class CanalShopCacheIntegrationTest {

    private static final Long SHOP_ID = 1L;
    private static final String SHOP_CACHE_KEY = CACHE_SHOP_KEY + SHOP_ID;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private CanalClient canalClient;

    /**
     * 写入缓存后提交一次店铺名称修改，验证缓存最终被 Canal Consumer 删除。
     */
    @Test
    void shouldDeleteShopCacheAfterMysqlUpdate() throws InterruptedException {
        String originalName = jdbcTemplate.queryForObject(
                "SELECT name FROM tb_shop WHERE id = ?", String.class, SHOP_ID);
        assertTrue(waitUntil(canalClient::isConnected, Duration.ofSeconds(15)),
                "Canal Client did not connect in time");

        try {
            stringRedisTemplate.opsForValue().set(SHOP_CACHE_KEY, "canal-integration-test");
            assertTrue(Boolean.TRUE.equals(stringRedisTemplate.hasKey(SHOP_CACHE_KEY)));

            jdbcTemplate.update("UPDATE tb_shop SET name = ? WHERE id = ?", "test", SHOP_ID);

            assertTrue(waitUntil(
                    () -> Boolean.FALSE.equals(stringRedisTemplate.hasKey(SHOP_CACHE_KEY)),
                    Duration.ofSeconds(15)),
                    "Shop cache was not deleted after MySQL update");
            assertFalse(Boolean.TRUE.equals(stringRedisTemplate.hasKey(SHOP_CACHE_KEY)));
        } finally {
            // 恢复业务数据；恢复动作也会产生 binlog，重复 DEL 按幂等成功处理。
            jdbcTemplate.update("UPDATE tb_shop SET name = ? WHERE id = ?", originalName, SHOP_ID);
            stringRedisTemplate.delete(SHOP_CACHE_KEY);
        }
    }

    /**
     * 小间隔轮询异步结果，避免使用固定长时间 sleep。
     */
    private boolean waitUntil(Check check, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (check.matches()) {
                return true;
            }
            Thread.sleep(100L);
        }
        return check.matches();
    }

    @FunctionalInterface
    private interface Check {
        boolean matches();
    }
}
