package com.ajay.kafkademo.consumer;

import com.ajay.kafkademo.model.OrderEvent;
import com.ajay.kafkademo.webhook.WebhookSenderService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


@Component
@Slf4j
@AllArgsConstructor
public class OrderEventConsumer {

    /**
     * Simple in-memory idempotency guard (stand-in for a real "processed_events"
     * table or Redis set). Keyed by partition+offset so we can detect a
     * redelivery of the exact same record (e.g., after a rebalance or a retry)
     * and skip reprocessing its side effect.
     *
     * NOTE: in-memory only - resets on app restart. Fine for this demo, but a
     * real system needs a durable store so idempotency survives a crash too.
     */
    private final Set<String> processedEventKeys = ConcurrentHashMap.newKeySet();
    private final WebhookSenderService webhookSenderService;


    @KafkaListener(topics = "order-events", groupId = "order-processing-group")
    public void handleOrderEvent(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        String dedupeKey = record.partition() + "-" + record.offset();

        if (processedEventKeys.contains(dedupeKey)) {
            log.warn("Duplicate delivery detected for partition={} offset={}, skipping",
                    record.partition(), record.offset());
            ack.acknowledge();
            return;
        }

        OrderEvent event = record.value();
        try {
            log.info("Processing event={} from partition={} offset={}",
                    event, record.partition(), record.offset());

            processEvent(event);
            processedEventKeys.add(dedupeKey);
            // Only commit the offset AFTER processing succeeds.
            ack.acknowledge();

        } catch (Exception ex) {
            log.error("Failed to process event={}", event, ex);
            throw ex; // DefaultErrorHandler (KafkaConsumerConfig) retries, then routes to DLT.
        }
    }

    private void processEvent(OrderEvent event) {

        if ("FAIL_ME".equals(event.getStatus())) {
            throw new RuntimeException("Simulated processing failure for " + event.getOrderId());
        }

        webhookSenderService.sendWebhook(event);

    }


}