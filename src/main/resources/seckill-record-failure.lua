-- KEYS[1]：原始订单 Stream；KEYS[2]：失败订单 Stream；KEYS[3]：重试计数 Hash。
-- ARGV：消费者组、原消息 ID、订单 ID、用户 ID、优惠券 ID、重试次数、异常摘要、失败时间。

-- 先持久化失败上下文，供告警、人工排查和后续补偿使用。
redis.call(
    'xadd', KEYS[2], '*',
    'originalRecordId', ARGV[2],
    'orderId', ARGV[3],
    'userId', ARGV[4],
    'voucherId', ARGV[5],
    'retryCount', ARGV[6],
    'error', ARGV[7],
    'failedAt', ARGV[8]
)

-- 失败记录成功后才确认原消息，避免异常订单被静默丢弃。
local acknowledged = redis.call('xack', KEYS[1], ARGV[1], ARGV[2])

-- 原消息已结束处理，删除对应重试计数。
redis.call('hdel', KEYS[3], ARGV[2])

return acknowledged
