package com.ajay.kafkademo.producer;

import com.ajay.kafkademo.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final String TOPIC = "order-events";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Sends asynchronously (KafkaTemplate.send returns a CompletableFuture) rather
     * than blocking the calling thread. We attach callbacks to log success/failure -
     * this is the realistic pattern for a Spring service, as opposed to console tools.
     *
     * Key = orderId, same reasoning as Chapter 1: all events for one order land
     * on the same partition, so they're processed in strict order downstream.
     */
    public void sendOrderEvent(OrderEvent event) {
        String key = event.getOrderId();

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(TOPIC, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                var metadata = result.getRecordMetadata();
                log.info("Sent event={} to partition={} offset={}",
                        event, metadata.partition(), metadata.offset());
            } else {
                // In a real system: push to a retry topic / dead-letter / alert here.
                log.error("Failed to send event={}", event, ex);
            }
        });
    }

    public CompletableFuture<SendResult<String, OrderEvent>> sendOrderEventAsync(OrderEvent event) {
        return kafkaTemplate.send(TOPIC, event.getOrderId(), event);
    }
}
