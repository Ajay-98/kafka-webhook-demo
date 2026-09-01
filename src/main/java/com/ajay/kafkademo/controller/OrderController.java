package com.ajay.kafkademo.controller;

import com.ajay.kafkademo.model.OrderEvent;
import com.ajay.kafkademo.producer.OrderEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestParam;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderEventProducer orderEventProducer;

    public OrderController(OrderEventProducer orderEventProducer) {
        this.orderEventProducer = orderEventProducer;
    }

    /**
     * Try it:
     * curl -X POST http://localhost:8081/api/orders \
     *   -H "Content-Type: application/json" \
     *   -d '{"orderId":"order-1","status":"CREATED"}'
     */
    @PostMapping
    public ResponseEntity<String> publishOrderEvent(@RequestBody OrderEvent event) {
        orderEventProducer.sendOrderEvent(event);
        return ResponseEntity.accepted().body("Event submitted: " + event);
    }

    @PostMapping("/bulk")
    public ResponseEntity<String> publishBulk(@RequestParam(defaultValue = "200") int count) {
        long start = System.currentTimeMillis();

        List<CompletableFuture<?>> futures = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            OrderEvent event = new OrderEvent("bulk-order-" + i, "CREATED");
            futures.add(orderEventProducer.sendOrderEventAsync(event));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long elapsedMs = System.currentTimeMillis() - start;
        return ResponseEntity.ok("Sent " + count + " events in " + elapsedMs + " ms");
    }


}
