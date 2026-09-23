package com.hmdp.aiops;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * Read-only observation hooks around existing public method boundaries.
 * The aspect never changes arguments, return values, exceptions, or transaction state.
 */
@Aspect
@Component
@Profile({"web", "consumer"})
@ConditionalOnProperty(prefix = "hmdp.aiops.business-metrics", name = "enabled", havingValue = "true")
public class BusinessMetricsObservationAspect {

    private final BusinessMetricsRegistry registry;

    public BusinessMetricsObservationAspect(BusinessMetricsRegistry registry) {
        this.registry = registry;
    }

    /** Every invocation of the existing seckill entry service is one observed request. */
    @Around("execution(* com.hmdp.service.impl.VoucherOrderServiceImpl.seckillVoucher(..))")
    public Object observeSeckillRequest(ProceedingJoinPoint joinPoint) throws Throwable {
        registry.recordSeckillRequest();
        return joinPoint.proceed();
    }

    /**
     * The Producer is invoked only after the Lua result is successful in Kafka mode.
     * Broker-confirmed sends are counted from the existing future without replacing it.
     */
    @Around("execution(* com.hmdp.kafka.producer.VoucherOrderKafkaProducer.sendOrderMessage(..))")
    public Object observeKafkaSend(ProceedingJoinPoint joinPoint) throws Throwable {
        registry.recordLuaAdmissionSuccess();
        Object result = joinPoint.proceed();
        if (result instanceof ListenableFuture) {
            attachSendObservation((ListenableFuture<?>) result);
        }
        return result;
    }

    /** Listener success means the existing transaction returned and the offset may be acknowledged. */
    @Around("execution(* com.hmdp.kafka.consumer.VoucherOrderKafkaConsumer.listen*(..))")
    public Object observeOrderPersistence(ProceedingJoinPoint joinPoint) throws Throwable {
        try {
            Object result = joinPoint.proceed();
            registry.recordOrderCreatedSuccess();
            return result;
        } catch (Throwable exception) {
            registry.recordOrderCreatedFailure();
            throw exception;
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void attachSendObservation(ListenableFuture<?> future) {
        ((ListenableFuture) future).addCallback(new ListenableFutureCallback() {
            @Override
            public void onSuccess(Object result) {
                registry.recordKafkaMessageSent();
            }

            @Override
            public void onFailure(Throwable exception) {
                // Failure is represented by the gap between Lua admission and broker-confirmed sends.
            }
        });
    }
}
