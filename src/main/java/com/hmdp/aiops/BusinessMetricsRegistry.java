package com.hmdp.aiops;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/** JVM-local counters populated only by the read-only AIOps observation aspect. */
@Component
@Profile({"web", "consumer"})
@ConditionalOnProperty(prefix = "hmdp.aiops.business-metrics", name = "enabled", havingValue = "true")
public class BusinessMetricsRegistry {

    static final String WEB_ROLE = "hmdp-web";
    static final String CONSUMER_ROLE = "hmdp-consumer";

    private final Instant startedAt = Instant.now();
    private final LongAdder seckillRequestCount = new LongAdder();
    private final LongAdder luaAdmissionSuccessCount = new LongAdder();
    private final LongAdder kafkaMessageSentCount = new LongAdder();
    private final LongAdder orderCreatedSuccessCount = new LongAdder();
    private final LongAdder orderCreatedFailureCount = new LongAdder();

    public void recordSeckillRequest() {
        seckillRequestCount.increment();
    }

    public void recordLuaAdmissionSuccess() {
        luaAdmissionSuccessCount.increment();
    }

    public void recordKafkaMessageSent() {
        kafkaMessageSentCount.increment();
    }

    public void recordOrderCreatedSuccess() {
        orderCreatedSuccessCount.increment();
    }

    public void recordOrderCreatedFailure() {
        orderCreatedFailureCount.increment();
    }

    /** Return only the counters owned by the requested JVM role. */
    public Map<String, Object> snapshot(String sourceRole) {
        Map<String, Object> facts = new LinkedHashMap<>();
        facts.put("source_role", sourceRole);
        facts.put("started_at", startedAt.toString());
        facts.put("collected_at", Instant.now().toString());
        if (WEB_ROLE.equals(sourceRole)) {
            facts.put("seckill_request_count", seckillRequestCount.sum());
            facts.put("lua_admission_success_count", luaAdmissionSuccessCount.sum());
            facts.put("kafka_message_sent_count", kafkaMessageSentCount.sum());
        } else if (CONSUMER_ROLE.equals(sourceRole)) {
            facts.put("order_created_success_count", orderCreatedSuccessCount.sum());
            facts.put("order_created_failure_count", orderCreatedFailureCount.sum());
        } else {
            throw new IllegalArgumentException("unsupported business metrics source role");
        }
        return facts;
    }
}
