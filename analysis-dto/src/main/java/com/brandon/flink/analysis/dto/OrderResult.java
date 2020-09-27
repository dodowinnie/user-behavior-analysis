package com.brandon.flink.analysis.dto;

public class OrderResult {

    public Long orderId;

    public String resultMsg;

    public OrderResult(Long orderId, String resultMsg) {
        this.orderId = orderId;
        this.resultMsg = resultMsg;
    }

    public OrderResult() {
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId=" + orderId +
                ", resultMsg='" + resultMsg + '\'' +
                '}';
    }
}
