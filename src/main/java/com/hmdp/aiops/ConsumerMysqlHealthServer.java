package com.hmdp.aiops;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Optional loopback-only, read-only health outlet owned by the Consumer JVM. */
@Component
@Profile("consumer")
@ConditionalOnProperty(prefix = "hmdp.aiops.mysql-health", name = "enabled", havingValue = "true")
public class ConsumerMysqlHealthServer implements SmartLifecycle {

    static final String PATH = "/internal/aiops/mysql-health";

    private final DataSource dataSource;
    private final ObjectMapper mapper;
    private final int port;
    private final int probeTimeoutMillis;
    private volatile HttpServer server;
    private volatile ExecutorService httpExecutor;
    private volatile ExecutorService probeExecutor;

    public ConsumerMysqlHealthServer(DataSource dataSource, ObjectMapper mapper,
            @Value("${hmdp.aiops.mysql-health.port:18082}") int port,
            @Value("${hmdp.aiops.mysql-health.probe-timeout-ms:1500}") int probeTimeoutMillis) {
        if (port < 1 || port > 65535 || probeTimeoutMillis < 100 || probeTimeoutMillis > 5000) {
            throw new IllegalArgumentException("invalid MySQL health outlet limits");
        }
        this.dataSource = dataSource;
        this.mapper = mapper;
        this.port = port;
        this.probeTimeoutMillis = probeTimeoutMillis;
    }

    @Override
    public synchronized void start() {
        if (server != null) {
            return;
        }
        try {
            probeExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new SynchronousQueue<>(), task -> {
                        Thread thread = new Thread(task, "aiops-mysql-probe");
                        thread.setDaemon(true);
                        return thread;
                    }, new ThreadPoolExecutor.AbortPolicy());
            httpExecutor = java.util.concurrent.Executors.newFixedThreadPool(2, task -> {
                Thread thread = new Thread(task, "aiops-mysql-health-http");
                thread.setDaemon(true);
                return thread;
            });
            HttpServer created = HttpServer.create(
                    new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port), 0);
            created.createContext(PATH, this::handle);
            created.setExecutor(httpExecutor);
            created.start();
            server = created;
        } catch (IOException exception) {
            stop();
            throw new IllegalStateException("cannot start Consumer MySQL health outlet", exception);
        }
    }

    @Override
    public synchronized void stop() {
        HttpServer active = server;
        server = null;
        if (active != null) {
            active.stop(0);
        }
        if (httpExecutor != null) {
            httpExecutor.shutdownNow();
            httpExecutor = null;
        }
        if (probeExecutor != null) {
            probeExecutor.shutdownNow();
            probeExecutor = null;
        }
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return server != null;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private void handle(HttpExchange exchange) throws IOException {
        try {
            if (!PATH.equals(exchange.getRequestURI().getPath())) {
                send(exchange, 404, new byte[0]);
                return;
            }
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                send(exchange, 405, new byte[0]);
                return;
            }
            send(exchange, 200, mapper.writeValueAsBytes(snapshot()));
        } catch (RuntimeException exception) {
            // Never echo JDBC configuration, SQL, or exception text to the caller.
            send(exchange, 500, new byte[0]);
        } finally {
            exchange.close();
        }
    }

    private static void send(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream output = exchange.getResponseBody()) {
                output.write(body);
            }
        }
    }

    /** Snapshot contains only pool and connection-validation facts. */
    Map<String, Object> snapshot() {
        Map<String, Object> facts = new LinkedHashMap<>();
        facts.put("source_role", "hmdp-consumer");
        facts.put("collected_at", Instant.now().toString());
        String testStatus = "unavailable";
        Boolean reachable = null;
        ExecutorService executor = probeExecutor;
        if (executor != null) {
            try {
                Future<Boolean> probe = executor.submit(() -> {
                    try (Connection connection = dataSource.getConnection()) {
                        return connection.isValid(1);
                    }
                });
                try {
                    reachable = probe.get(probeTimeoutMillis, TimeUnit.MILLISECONDS);
                    testStatus = reachable ? "valid" : "invalid";
                } catch (TimeoutException exception) {
                    probe.cancel(true);
                    testStatus = "timeout";
                }
            } catch (RejectedExecutionException exception) {
                testStatus = "busy";
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                testStatus = "interrupted";
            } catch (ExecutionException exception) {
                Throwable cause = exception.getCause();
                if (cause instanceof SQLTimeoutException) {
                    testStatus = "timeout";
                } else if (cause instanceof SQLException) {
                    String sqlState = ((SQLException) cause).getSQLState();
                    if (sqlState != null && sqlState.startsWith("08")) {
                        reachable = false;
                        testStatus = "connection_failed";
                    } else {
                        testStatus = "acquisition_error";
                    }
                } else {
                    testStatus = "probe_error";
                }
            }
        }
        facts.put("database_reachable", reachable);
        facts.put("connection_test_status", testStatus);

        Integer active = null;
        Integer idle = null;
        try {
            if (dataSource instanceof HikariDataSource) {
                HikariPoolMXBean pool = ((HikariDataSource) dataSource).getHikariPoolMXBean();
                if (pool != null) {
                    active = pool.getActiveConnections();
                    idle = pool.getIdleConnections();
                }
            }
        } catch (RuntimeException ignored) {
            // A failed pool-stat read must not hide the connection-test outcome.
            active = null;
            idle = null;
        }
        facts.put("hikari_active", active);
        facts.put("hikari_idle", idle);
        // HikariPoolMXBean does not expose cumulative timeout/business error counts.
        facts.put("connection_timeout_count", null);
        facts.put("error_count", null);
        List<String> unavailable = new ArrayList<>();
        if (active == null) unavailable.add("hikari_active");
        if (idle == null) unavailable.add("hikari_idle");
        unavailable.add("connection_timeout_count");
        unavailable.add("error_count");
        facts.put("unavailable_metrics", unavailable);
        return facts;
    }
}
