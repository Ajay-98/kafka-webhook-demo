package com.ajay.kafkademo.model;

/**
 * Same shape of event we sent manually via the console producer in Chapter 1,
 * now as a proper Java object we'll serialize to JSON automatically.
 */
public class OrderEvent {

    private String orderId;
    private String status; // CREATED, SHIPPED, DELIVERED, CANCELLED

    public OrderEvent() {
    }

    public OrderEvent(String orderId, String status) {
        this.orderId = orderId;
        this.status = status;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
