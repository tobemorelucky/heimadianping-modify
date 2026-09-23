package com.hmdp.aiops;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Loopback-only GET endpoint exposing the current JVM role's in-memory counters. */
@Component
@Profile({"web", "consumer"})
@ConditionalOnProperty(prefix = "hmdp.aiops.business-metrics", name = "enabled", havingValue = "true")
public class BusinessMetricsObservationServer implements SmartLifecycle {

    static final String WEB_PATH = "/internal/aiops/business-metrics/web";
    static final String CONSUMER_PATH = "/internal/aiops/business-metrics/consumer";

    private final BusinessMetricsRegistry registry;
    private final ObjectMapper mapper;
    private final String sourceRole;
    private final int port;
    private final String path;
    private volatile HttpServer server;
    private volatile ExecutorService executor;

    public BusinessMetricsObservationServer(
            BusinessMetricsRegistry registry,
            ObjectMapper mapper,
            @Value("${hmdp.aiops.business-metrics.role}") String sourceRole,
            @Value("${hmdp.aiops.business-metrics.port}") int port) {
        if (!BusinessMetricsRegistry.WEB_ROLE.equals(sourceRole)
                && !BusinessMetricsRegistry.CONSUMER_ROLE.equals(sourceRole)) {
            throw new IllegalArgumentException("invalid business metrics source role");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("invalid business metrics port");
        }
        this.registry = registry;
        this.mapper = mapper;
        this.sourceRole = sourceRole;
        this.port = port;
        this.path = BusinessMetricsRegistry.WEB_ROLE.equals(sourceRole) ? WEB_PATH : CONSUMER_PATH;
    }

    @Override
    public synchronized void start() {
        if (server != null) {
            return;
        }
        try {
            executor = Executors.newFixedThreadPool(2, task -> {
                Thread thread = new Thread(task, "aiops-business-metrics-http");
                thread.setDaemon(true);
                return thread;
            });
            HttpServer created = HttpServer.create(
                    new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port), 0);
            created.createContext(path, this::handle);
            created.setExecutor(executor);
            created.start();
            server = created;
        } catch (IOException exception) {
            stop();
            throw new IllegalStateException("cannot start business metrics outlet", exception);
        }
    }

    @Override
    public synchronized void stop() {
        HttpServer active = server;
        server = null;
        if (active != null) {
            active.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
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
            if (!path.equals(exchange.getRequestURI().getPath())) {
                send(exchange, 404, new byte[0]);
                return;
            }
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Allow", "GET");
                send(exchange, 405, new byte[0]);
                return;
            }
            send(exchange, 200, mapper.writeValueAsBytes(registry.snapshot(sourceRole)));
        } catch (RuntimeException exception) {
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
}
