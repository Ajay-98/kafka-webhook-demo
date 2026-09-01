package com.ajay.kafkademo.outbox;

import com.ajay.kafkademo.model.OrderEvent;
import com.ajay.kafkademo.producer.OrderEventProducer;
import com.ajay.kafkademo.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Polls outbox_events for unpublished rows, publishes each to Kafka via the
 * existing OrderEventProducer, and marks it published on success.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller {

    private final OutboxEventRepository outboxEventRepository;
    private final OrderEventProducer orderEventProducer;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 3000) // every 3 seconds
    public void pollAndPublish() {
        List<OutboxEvent> unpublished = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc();

        if (unpublished.isEmpty()) {
            return;
        }

        log.info("Found {} unpublished outbox event(s)", unpublished.size());

        for (OutboxEvent outboxEvent : unpublished) {
            try {
                OrderEvent event = objectMapper.readValue(outboxEvent.getPayload(), OrderEvent.class);

                orderEventProducer.sendOrderEventAsync(event).whenComplete((result, ex) -> {
                    if (ex == null) {
                        outboxEvent.setPublished(true);
                        outboxEvent.setPublishedAt(Instant.now());
                        outboxEventRepository.save(outboxEvent);
                        log.info("Published outbox event id={} orderId={} to partition={} offset={}",
                                outboxEvent.getId(), event.getOrderId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish outbox event id={}, will retry next poll",
                                outboxEvent.getId(), ex);
                    }
                });

            } catch (Exception e) {
                log.error("Failed to deserialize/publish outbox event id={}", outboxEvent.getId(), e);
            }
        }
    }
}