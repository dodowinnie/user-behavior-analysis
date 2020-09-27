package com.brandon.order;

import com.brandon.flink.analysis.dto.OrderEvent;
import com.brandon.flink.analysis.dto.OrderResult;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;

public class OrderPaySelect implements PatternSelectFunction<OrderEvent, OrderResult> {
    @Override
    public OrderResult select(Map<String, List<OrderEvent>> pattern) throws Exception {
        OrderEvent pay = pattern.get("pay").get(0);
        return new OrderResult(pay.orderId, "payed successfully");
    }
}
