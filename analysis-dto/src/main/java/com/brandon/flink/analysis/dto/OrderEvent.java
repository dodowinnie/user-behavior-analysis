package com.brandon.flink.analysis.dto;

public class OrderEvent {

    public Long orderId;

    public String eventType;

    public String txId;

    public Long timestamp;

    public OrderEvent(Long orderId, String eventType, String txId, Long timestamp) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.txId = txId;
        this.timestamp = timestamp;
    }

    public OrderEvent() {
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", txId='" + txId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
