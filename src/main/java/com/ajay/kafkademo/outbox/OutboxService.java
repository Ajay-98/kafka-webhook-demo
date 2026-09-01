package com.ajay.kafkademo.outbox;

import com.ajay.kafkademo.model.OrderEvent;
import com.ajay.kafkademo.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

@Service
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OutboxService(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public void recordOutboxEvent(OrderEvent orderEvent) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(orderEvent);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize order event: " + orderEvent, e);
        }

        OutboxEvent outboxEvent = new OutboxEvent(orderEvent.getOrderId(), orderEvent.getStatus(), payload);
        outboxEventRepository.save(outboxEvent);
    }
}
