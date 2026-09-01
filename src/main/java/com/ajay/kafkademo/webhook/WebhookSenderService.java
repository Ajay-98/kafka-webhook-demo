package com.ajay.kafkademo.webhook;

import com.ajay.kafkademo.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

/**
 * Sends the actual webhook HTTP call to webhook-receiver-demo. This is the
 * final hop of the whole system:
 *
 *   outbox -> Kafka -> OrderEventConsumer -> THIS -> webhook-receiver-demo
 *
 * Blocking on purpose: the Kafka consumer needs a definitive success/failure
 * result before it can decide whether to ack the offset (success) or let
 * the error handler retry (failure) - same "only ack after genuine success"
 * principle from Chapter 3, just extended one hop further to an HTTP call.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class WebhookSenderService {

    private final WebClient webClient;
    private final WebhookSigner signer;
    private final ObjectMapper objectMapper;

    @Value("${webhook.receiver-url}")
    private String receiverUrl;

    public void sendWebhook(OrderEvent event) {
        String jsonBody;
        try {
            jsonBody = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize event for webhook", e);
        }

        String signature = signer.sign(jsonBody);
        String webhookId = UUID.randomUUID().toString();
        long timestamp = Instant.now().getEpochSecond();

        webClient.post()
                .uri(receiverUrl)
                .header("Content-Type", "application/json")
                .header("X-Webhook-Signature", signature)
                .header("X-Webhook-Id", webhookId)
                .header("X-Webhook-Timestamp", String.valueOf(timestamp))
                .bodyValue(jsonBody)
                .retrieve()
                .toBodilessEntity()
                .timeout(Duration.ofSeconds(5))
                .block(); // Deliberate: see class-level comment.

        log.info("Webhook delivered successfully: id={} event={}", webhookId, event);
    }
}