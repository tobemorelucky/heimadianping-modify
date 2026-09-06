package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.entity.VoucherOrder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_FAILED_STREAM_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_MAX_RETRY_COUNT;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_RETRY_KEY;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_STREAM_GROUP;
import static com.hmdp.utils.RedisConstants.SECKILL_ORDER_STREAM_KEY;

/**
 * Redis Stream 秒杀订单消费者。
 *
 * <p>订单只在数据库事务成功后 ACK；连续失败达到上限后，使用 Lua 原子地
 * 写入失败 Stream 并确认原消息，避免消息静默丢失。</p>
 */
@Slf4j
@Component
public class VoucherOrderStreamConsumer {

    /** 失败记录和原消息 ACK 由同一个 Redis Lua 脚本原子完成。 */
    private static final DefaultRedisScript<Long> RECORD_FAILURE_SCRIPT;

    /** Redis 故障或处理失败后的短暂退避，避免空转打满 CPU。 */
    private static final long RETRY_BACKOFF_MILLIS = 500L;

    static {
        RECORD_FAILURE_SCRIPT = new DefaultRedisScript<>();
        RECORD_FAILURE_SCRIPT.setLocation(new ClassPathResource("seckill-record-failure.lua"));
        RECORD_FAILURE_SCRIPT.setResultType(Long.class);
    }

    private final StringRedisTemplate stringRedisTemplate;
    private final RedissonClient redissonClient;
    private final VoucherOrderTransactionalService transactionalService;

    /** 使用有名称的单线程，保持现有订单消费顺序并便于排查线程。 */
    private final ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "voucher-order-stream-consumer");
        thread.setDaemon(true);
        return thread;
    });

    /** 消费者名应在同一消费者组内唯一，并在实例重启后保持稳定以恢复 Pending。 */
    @Value("${hmdp.seckill.consumer-name:c1}")
    private String consumerName;

    /** 控制消费者线程优雅退出。 */
    private volatile boolean running = true;

    /** 避免每次轮询都重复创建消费者组。 */
    private volatile boolean consumerGroupReady;

    /**
     * 构造 Stream 消费者。
     */
    public VoucherOrderStreamConsumer(StringRedisTemplate stringRedisTemplate,
                                      RedissonClient redissonClient,
                                      VoucherOrderTransactionalService transactionalService) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.redissonClient = redissonClient;
        this.transactionalService = transactionalService;
    }

    /**
     * Spring 容器就绪后启动后台消费线程。
     */
    @PostConstruct
    public void start() {
        executor.submit(this::consumeLoop);
    }

    /**
     * Spring 容器关闭时停止消费，避免线程泄漏。
     */
    @PreDestroy
    public void stop() {
        running = false;
        executor.shutdownNow();
    }

    /**
     * 主循环优先恢复当前消费者的 Pending 消息，再读取新消息。
     */
    private void consumeLoop() {
        while (running) {
            try {
                ensureConsumerGroup();
                if (!drainPendingList()) {
                    sleepBeforeRetry();
                    continue;
                }
                consumeNewMessage();
            } catch (Exception e) {
                if (containsMessage(e, "NOGROUP")) {
                    consumerGroupReady = false;
                }
                if (running) {
                    log.error("秒杀订单消费循环异常，consumer={}", consumerName, e);
                    sleepBeforeRetry();
                }
            }
        }
    }

    /**
     * 使用 XGROUP CREATE ... MKSTREAM 自动准备 Stream 和消费者组。
     */
    private void ensureConsumerGroup() {
        if (consumerGroupReady) {
            return;
        }
        byte[] streamKey = stringRedisTemplate.getStringSerializer().serialize(SECKILL_ORDER_STREAM_KEY);
        try {
            stringRedisTemplate.execute((RedisCallback<String>) connection ->
                    connection.streamCommands().xGroupCreate(
                            streamKey,
                            SECKILL_ORDER_STREAM_GROUP,
                            ReadOffset.from("0"),
                            true
                    )
            );
            log.info("已创建秒杀订单消费者组，stream={}, group={}",
                    SECKILL_ORDER_STREAM_KEY, SECKILL_ORDER_STREAM_GROUP);
        } catch (DataAccessException e) {
            // BUSYGROUP 表示组已经存在，可安全继续；其他 Redis 异常必须重试。
            if (!containsMessage(e, "BUSYGROUP")) {
                throw e;
            }
        }
        consumerGroupReady = true;
    }

    /**
     * 阻塞读取一条新消息。
     */
    private void consumeNewMessage() {
        List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                Consumer.from(SECKILL_ORDER_STREAM_GROUP, consumerName),
                StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                StreamOffset.create(SECKILL_ORDER_STREAM_KEY, ReadOffset.lastConsumed())
        );
        if (records != null && !records.isEmpty()) {
            processRecord(records.get(0));
        }
    }

    /**
     * 恢复当前消费者尚未确认的消息；失败未到上限时保留在 Pending 中等待下轮重试。
     *
     * @return true 表示 Pending 已排空或消息已处理，false 表示需要退避后重试
     */
    private boolean drainPendingList() {
        while (running) {
            List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                    Consumer.from(SECKILL_ORDER_STREAM_GROUP, consumerName),
                    StreamReadOptions.empty().count(1),
                    StreamOffset.create(SECKILL_ORDER_STREAM_KEY, ReadOffset.from("0"))
            );
            if (records == null || records.isEmpty()) {
                return true;
            }
            if (!processRecord(records.get(0))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 执行用户和优惠券维度加锁、数据库事务和 ACK。
     */
    private boolean processRecord(MapRecord<String, Object, Object> record) {
        VoucherOrder voucherOrder = new VoucherOrder();
        RLock redisLock = null;
        boolean locked = false;
        try {
            // 消息解析也属于消费过程，格式异常必须进入有限重试与失败记录。
            voucherOrder = BeanUtil.fillBeanWithMap(record.getValue(), voucherOrder, true);
            redisLock = redissonClient.getLock(
                    "lock:order:" + voucherOrder.getUserId() + ":" + voucherOrder.getVoucherId()
            );
            locked = redisLock.tryLock();
            if (!locked) {
                throw new IllegalStateException("未获取到秒杀订单处理锁");
            }

            // 该调用经过独立 Spring Bean 的事务代理。
            transactionalService.createVoucherOrder(voucherOrder);
            acknowledge(record.getId());
            clearRetryCounter(record.getId());
            log.info("秒杀订单处理成功，recordId={}, orderId={}, userId={}, voucherId={}",
                    record.getId().getValue(), voucherOrder.getId(),
                    voucherOrder.getUserId(), voucherOrder.getVoucherId());
            return true;
        } catch (Exception e) {
            return handleFailure(record, voucherOrder, e);
        } finally {
            if (locked && redisLock != null && redisLock.isHeldByCurrentThread()) {
                try {
                    redisLock.unlock();
                } catch (Exception unlockException) {
                    // 释放失败由 Redisson watchdog/租约兜底，同时保留日志用于告警。
                    log.error("释放秒杀订单锁失败，orderId={}, userId={}, voucherId={}",
                            voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId(),
                            unlockException);
                }
            }
        }
    }

    /**
     * 对实际源 Stream 执行 ACK，并校验 Redis 确认结果。
     */
    void acknowledge(RecordId recordId) {
        Long acknowledged = stringRedisTemplate.opsForStream().acknowledge(
                SECKILL_ORDER_STREAM_KEY,
                SECKILL_ORDER_STREAM_GROUP,
                recordId
        );
        if (acknowledged == null || acknowledged != 1L) {
            throw new IllegalStateException("秒杀订单消息 ACK 失败，recordId=" + recordId.getValue());
        }
    }

    /**
     * 增加持久化重试计数；达到上限后记录失败订单并确认原消息。
     */
    private boolean handleFailure(MapRecord<String, Object, Object> record,
                                  VoucherOrder voucherOrder,
                                  Exception processingException) {
        try {
            Long retryCount = stringRedisTemplate.opsForHash().increment(
                    SECKILL_ORDER_RETRY_KEY,
                    record.getId().getValue(),
                    1L
            );
            long attempts = retryCount == null ? 1L : retryCount;
            if (attempts < SECKILL_ORDER_MAX_RETRY_COUNT) {
                log.warn("秒杀订单处理失败，将保留 Pending 重试，recordId={}, orderId={}, retry={}/{}",
                        record.getId().getValue(), voucherOrder.getId(),
                        attempts, SECKILL_ORDER_MAX_RETRY_COUNT, processingException);
                return false;
            }

            // Lua 保证“写失败 Stream”和“ACK 原消息”在 Redis 内原子完成。
            Long acknowledged = stringRedisTemplate.execute(
                    RECORD_FAILURE_SCRIPT,
                    Arrays.asList(
                            SECKILL_ORDER_STREAM_KEY,
                            SECKILL_ORDER_FAILED_STREAM_KEY,
                            SECKILL_ORDER_RETRY_KEY
                    ),
                    SECKILL_ORDER_STREAM_GROUP,
                    record.getId().getValue(),
                    valueOf(voucherOrder.getId()),
                    valueOf(voucherOrder.getUserId()),
                    valueOf(voucherOrder.getVoucherId()),
                    String.valueOf(attempts),
                    safeErrorMessage(processingException),
                    Instant.now().toString()
            );
            if (acknowledged == null || acknowledged != 1L) {
                throw new IllegalStateException("失败订单记录后 ACK 原消息失败");
            }
            log.error("秒杀订单达到最大重试次数，已记录失败 Stream，recordId={}, orderId={}, retry={}",
                    record.getId().getValue(), voucherOrder.getId(), attempts, processingException);
            return true;
        } catch (Exception failureRecordingException) {
            // 失败记录未成功时绝不 ACK，原消息继续保留在 Pending List。
            log.error("记录失败秒杀订单异常，原消息保持 Pending，recordId={}, orderId={}",
                    record.getId().getValue(), voucherOrder.getId(), failureRecordingException);
            return false;
        }
    }

    /**
     * ACK 成功后尽力清理历史重试计数；清理失败不改变已提交订单状态。
     */
    private void clearRetryCounter(RecordId recordId) {
        try {
            stringRedisTemplate.opsForHash().delete(SECKILL_ORDER_RETRY_KEY, recordId.getValue());
        } catch (Exception e) {
            log.warn("清理秒杀订单重试计数失败，recordId={}", recordId.getValue(), e);
        }
    }

    /**
     * 失败重试前短暂休眠，并响应容器关闭中断。
     */
    private void sleepBeforeRetry() {
        try {
            TimeUnit.MILLISECONDS.sleep(RETRY_BACKOFF_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    /**
     * 截断写入失败 Stream 的异常描述，完整堆栈仍保留在应用日志中。
     */
    private String safeErrorMessage(Exception exception) {
        String message = exception.getClass().getSimpleName() + ": " + exception.getMessage();
        return message.length() <= 500 ? message : message.substring(0, 500);
    }

    /**
     * 将可空订单字段转换为 Lua 参数。
     */
    private String valueOf(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    /**
     * 递归检查 Redis 包装异常中的标志文本。
     */
    private boolean containsMessage(Throwable throwable, String expected) {
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(expected)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
