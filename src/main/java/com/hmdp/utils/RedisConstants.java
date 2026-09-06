package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    /** 秒杀订单主 Stream。 */
    public static final String SECKILL_ORDER_STREAM_KEY = "stream.orders";
    /** 秒杀订单消费者组。 */
    public static final String SECKILL_ORDER_STREAM_GROUP = "g1";
    /** 达到最大重试次数后的失败订单 Stream。 */
    public static final String SECKILL_ORDER_FAILED_STREAM_KEY = "stream.orders.failed";
    /** 按原始消息 ID 记录消费失败次数的 Hash。 */
    public static final String SECKILL_ORDER_RETRY_KEY = "seckill:order:retry";
    /** 单条订单消息的最大处理次数。 */
    public static final int SECKILL_ORDER_MAX_RETRY_COUNT = 3;
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
}
