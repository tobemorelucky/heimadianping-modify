package com.hmdp.runtime;

import com.hmdp.config.kafka.KafkaProducerConfig;
import com.hmdp.kafka.consumer.VoucherOrderDltConsumer;
import com.hmdp.kafka.consumer.VoucherOrderKafkaConsumer;
import com.hmdp.service.impl.VoucherOrderStreamConsumer;
import com.hmdp.service.impl.VoucherOrderTransactionalService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Verifies that the two runtime profiles isolate background consumers without changing the legacy default. */
class DualRuntimeProfileTest {

    private ApplicationContextRunner roleContext(String profile) {
        return new ApplicationContextRunner()
                .withInitializer(context -> {
                    YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
                    try {
                        for (PropertySource<?> source : loader.load(
                                profile, new ClassPathResource("application-" + profile + ".yaml"))) {
                            context.getEnvironment().getPropertySources().addFirst(source);
                        }
                    } catch (IOException exception) {
                        throw new UncheckedIOException(exception);
                    }
                })
                .withPropertyValues(
                        "spring.kafka.bootstrap-servers=127.0.0.1:9092",
                        "hmdp.kafka.topics.voucher-order=hmdp.seckill.order.create.v1")
                .withBean(VoucherOrderTransactionalService.class,
                        () -> mock(VoucherOrderTransactionalService.class))
                .withUserConfiguration(RoleBeans.class);
    }

    @Test
    void webProfileKeepsProducerButRegistersNoOrderConsumers() {
        roleContext("web").run(context -> {
            assertTrue(context.getBeansOfType(VoucherOrderKafkaConsumer.class).isEmpty());
            assertTrue(context.getBeansOfType(VoucherOrderDltConsumer.class).isEmpty());
            assertTrue(context.getBeansOfType(VoucherOrderStreamConsumer.class).isEmpty());
            assertTrue(context.getBeansOfType(KafkaTemplate.class).size() > 0);
            assertEquals("hmdp-web", context.getEnvironment().getProperty("spring.application.name"));
            assertEquals("kafka", context.getEnvironment().getProperty("seckill.message.mode"));
        });
    }

    @Test
    void consumerProfileRegistersKafkaListenersWithoutWebServer() {
        roleContext("consumer").run(context -> {
            assertEquals(1, context.getBeansOfType(VoucherOrderKafkaConsumer.class).size());
            assertEquals(1, context.getBeansOfType(VoucherOrderDltConsumer.class).size());
            assertTrue(context.getBeansOfType(VoucherOrderStreamConsumer.class).isEmpty());
            assertTrue(context.getBeansOfType(KafkaTemplate.class).size() > 0);
            assertEquals("none", context.getEnvironment().getProperty("spring.main.web-application-type"));
            assertEquals("false", context.getEnvironment().getProperty("canal.enabled"));
            assertEquals("hmdp-consumer", context.getEnvironment().getProperty("spring.application.name"));
            assertEquals("kafka", context.getEnvironment().getProperty("seckill.message.mode"));
        });
    }

    @Test
    void legacyDefaultStillRegistersKafkaListeners() {
        new ApplicationContextRunner()
                .withBean(VoucherOrderTransactionalService.class,
                        () -> mock(VoucherOrderTransactionalService.class))
                .withUserConfiguration(KafkaListenersOnly.class)
                .run(context -> {
                    assertEquals(1, context.getBeansOfType(VoucherOrderKafkaConsumer.class).size());
                    assertEquals(1, context.getBeansOfType(VoucherOrderDltConsumer.class).size());
                });
    }

    @Configuration
    @Import({KafkaProducerConfig.class, VoucherOrderKafkaConsumer.class,
            VoucherOrderDltConsumer.class, VoucherOrderStreamConsumer.class})
    static class RoleBeans {
    }

    @Configuration
    @Import({VoucherOrderKafkaConsumer.class, VoucherOrderDltConsumer.class})
    static class KafkaListenersOnly {
    }
}
