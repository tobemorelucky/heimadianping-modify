package com.hmdp.aiops;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;
import java.net.ServerSocket;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLNonTransientConnectionException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumerMysqlHealthServerTest {

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void reportsConsumerConnectionAndActualHikariSnapshotWithoutBusinessData() throws Exception {
        HikariDataSource dataSource = mock(HikariDataSource.class);
        HikariPoolMXBean pool = mock(HikariPoolMXBean.class);
        Connection connection = mock(Connection.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.isValid(1)).thenReturn(true);
        when(dataSource.getHikariPoolMXBean()).thenReturn(pool);
        when(pool.getActiveConnections()).thenReturn(2);
        when(pool.getIdleConnections()).thenReturn(3);
        ConsumerMysqlHealthServer health = new ConsumerMysqlHealthServer(
                dataSource, new ObjectMapper(), freePort(), 1000);
        health.start();
        try {
            Map<String, Object> facts = health.snapshot();
            assertEquals("hmdp-consumer", facts.get("source_role"));
            assertEquals(true, facts.get("database_reachable"));
            assertEquals("valid", facts.get("connection_test_status"));
            assertEquals(2, facts.get("hikari_active"));
            assertEquals(3, facts.get("hikari_idle"));
            assertNull(facts.get("connection_timeout_count"));
            assertNull(facts.get("error_count"));
            assertFalse(facts.containsKey("jdbc_url"));
            assertFalse(facts.containsKey("orders"));
        } finally {
            health.stop();
        }
    }

    @Test
    void reportsConnectionFailureWithoutLeakingDriverErrorOrInventingPoolMetrics() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenThrow(
                new SQLNonTransientConnectionException("jdbc secret", "08001"));
        ConsumerMysqlHealthServer health = new ConsumerMysqlHealthServer(
                dataSource, new ObjectMapper(), freePort(), 1000);
        health.start();
        try {
            Map<String, Object> facts = health.snapshot();
            assertEquals(false, facts.get("database_reachable"));
            assertEquals("connection_failed", facts.get("connection_test_status"));
            assertNull(facts.get("hikari_active"));
            assertNull(facts.get("hikari_idle"));
            assertFalse(new ObjectMapper().writeValueAsString(facts).contains("jdbc secret"));
        } finally {
            health.stop();
        }
    }

    @Test
    void loopbackEndpointIsGetOnlyAndReturnsNoBusinessFields() throws Exception {
        int port = freePort();
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.isValid(1)).thenReturn(true);
        ConsumerMysqlHealthServer health = new ConsumerMysqlHealthServer(
                dataSource, new ObjectMapper(), port, 1000);
        health.start();
        try {
            URL url = new URL("http://127.0.0.1:" + port + ConsumerMysqlHealthServer.PATH);
            HttpURLConnection get = (HttpURLConnection) url.openConnection();
            assertEquals(200, get.getResponseCode());
            Map<?, ?> body = new ObjectMapper().readValue(get.getInputStream(), Map.class);
            assertEquals("hmdp-consumer", body.get("source_role"));
            assertEquals(true, body.get("database_reachable"));
            assertFalse(body.containsKey("orders"));
            assertFalse(body.containsKey("password"));
            get.disconnect();

            HttpURLConnection post = (HttpURLConnection) url.openConnection();
            post.setRequestMethod("POST");
            assertEquals(405, post.getResponseCode());
            post.disconnect();
        } finally {
            health.stop();
        }
    }

    @Test
    void boundedProbeTimeoutDoesNotClaimDatabaseIsUnreachable() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenAnswer(invocation -> {
            Thread.sleep(1000);
            return mock(Connection.class);
        });
        ConsumerMysqlHealthServer health = new ConsumerMysqlHealthServer(
                dataSource, new ObjectMapper(), freePort(), 100);
        health.start();
        try {
            Map<String, Object> facts = health.snapshot();
            assertEquals("timeout", facts.get("connection_test_status"));
            assertNull(facts.get("database_reachable"));
        } finally {
            health.stop();
        }
    }

    @Test
    void outletRequiresConsumerProfileAndExplicitEnablement() throws Exception {
        int port = freePort();
        ApplicationContextRunner runner = new ApplicationContextRunner()
                .withBean(DataSource.class, () -> mock(DataSource.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withUserConfiguration(HealthOutletBeans.class)
                .withPropertyValues(
                        "hmdp.aiops.mysql-health.enabled=true",
                        "hmdp.aiops.mysql-health.port=" + port);
        runner.withInitializer(context -> context.getEnvironment().setActiveProfiles("web"))
                .run(context -> assertTrue(context.getBeansOfType(ConsumerMysqlHealthServer.class).isEmpty()));
        runner.withInitializer(context -> context.getEnvironment().setActiveProfiles("consumer"))
                .withPropertyValues("hmdp.aiops.mysql-health.enabled=false")
                .run(context -> assertTrue(context.getBeansOfType(ConsumerMysqlHealthServer.class).isEmpty()));
        runner.withInitializer(context -> context.getEnvironment().setActiveProfiles("consumer"))
                .run(context -> assertEquals(1, context.getBeansOfType(ConsumerMysqlHealthServer.class).size()));
    }

    @Configuration
    @Import(ConsumerMysqlHealthServer.class)
    static class HealthOutletBeans {
    }
}
