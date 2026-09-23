package com.hmdp.aiops;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.aspectj.lang.ProceedingJoinPoint;
import org.junit.jupiter.api.Test;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BusinessMetricsObservationTest {

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void webObservationCountsRequestLuaAdmissionAndBrokerConfirmation() throws Throwable {
        BusinessMetricsRegistry registry = new BusinessMetricsRegistry();
        BusinessMetricsObservationAspect aspect = new BusinessMetricsObservationAspect(registry);
        ProceedingJoinPoint request = mock(ProceedingJoinPoint.class);
        when(request.proceed()).thenReturn("accepted");

        assertEquals("accepted", aspect.observeSeckillRequest(request));

        SettableListenableFuture<Object> future = new SettableListenableFuture<>();
        ProceedingJoinPoint producer = mock(ProceedingJoinPoint.class);
        when(producer.proceed()).thenReturn(future);
        assertSame(future, aspect.observeKafkaSend(producer));

        Map<String, Object> beforeAck = registry.snapshot(BusinessMetricsRegistry.WEB_ROLE);
        assertEquals(1L, beforeAck.get("seckill_request_count"));
        assertEquals(1L, beforeAck.get("lua_admission_success_count"));
        assertEquals(0L, beforeAck.get("kafka_message_sent_count"));

        future.set(new Object());
        Map<String, Object> afterAck = registry.snapshot(BusinessMetricsRegistry.WEB_ROLE);
        assertEquals(1L, afterAck.get("kafka_message_sent_count"));
        assertFalse(afterAck.containsKey("order_created_success_count"));
    }

    @Test
    void consumerObservationPreservesSuccessFailureAndOriginalException() throws Throwable {
        BusinessMetricsRegistry registry = new BusinessMetricsRegistry();
        BusinessMetricsObservationAspect aspect = new BusinessMetricsObservationAspect(registry);
        ProceedingJoinPoint success = mock(ProceedingJoinPoint.class);
        when(success.proceed()).thenReturn(null);
        aspect.observeOrderPersistence(success);

        IllegalStateException failure = new IllegalStateException("persistence failed");
        ProceedingJoinPoint failed = mock(ProceedingJoinPoint.class);
        when(failed.proceed()).thenThrow(failure);
        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> aspect.observeOrderPersistence(failed));

        assertSame(failure, thrown);
        Map<String, Object> facts = registry.snapshot(BusinessMetricsRegistry.CONSUMER_ROLE);
        assertEquals(1L, facts.get("order_created_success_count"));
        assertEquals(1L, facts.get("order_created_failure_count"));
        assertFalse(facts.containsKey("seckill_request_count"));
    }

    @Test
    void webOutletIsLoopbackGetOnlyAndContainsNoBusinessRecords() throws Exception {
        BusinessMetricsRegistry registry = new BusinessMetricsRegistry();
        registry.recordSeckillRequest();
        registry.recordLuaAdmissionSuccess();
        registry.recordKafkaMessageSent();
        int port = freePort();
        BusinessMetricsObservationServer server = new BusinessMetricsObservationServer(
                registry,
                new ObjectMapper(),
                BusinessMetricsRegistry.WEB_ROLE,
                port);
        server.start();
        try {
            URL url = new URL("http://127.0.0.1:" + port
                    + BusinessMetricsObservationServer.WEB_PATH);
            HttpURLConnection get = (HttpURLConnection) url.openConnection();
            assertEquals(200, get.getResponseCode());
            Map<?, ?> body = new ObjectMapper().readValue(get.getInputStream(), Map.class);
            assertEquals("hmdp-web", body.get("source_role"));
            assertEquals(1, body.get("seckill_request_count"));
            assertEquals(1, body.get("lua_admission_success_count"));
            assertEquals(1, body.get("kafka_message_sent_count"));
            assertFalse(body.containsKey("orders"));
            assertFalse(body.containsKey("users"));
            get.disconnect();

            HttpURLConnection post = (HttpURLConnection) url.openConnection();
            post.setRequestMethod("POST");
            assertEquals(405, post.getResponseCode());
            post.disconnect();
        } finally {
            server.stop();
        }
    }

    @Test
    void consumerOutletReturnsOnlyConsumerCounters() throws Exception {
        BusinessMetricsRegistry registry = new BusinessMetricsRegistry();
        registry.recordOrderCreatedSuccess();
        int port = freePort();
        BusinessMetricsObservationServer server = new BusinessMetricsObservationServer(
                registry,
                new ObjectMapper(),
                BusinessMetricsRegistry.CONSUMER_ROLE,
                port);
        server.start();
        try {
            URL url = new URL("http://127.0.0.1:" + port
                    + BusinessMetricsObservationServer.CONSUMER_PATH);
            Map<?, ?> body = new ObjectMapper().readValue(url.openStream(), Map.class);
            assertEquals("hmdp-consumer", body.get("source_role"));
            assertEquals(1, body.get("order_created_success_count"));
            assertEquals(0, body.get("order_created_failure_count"));
            assertFalse(body.containsKey("kafka_message_sent_count"));
        } finally {
            server.stop();
        }
    }
}
