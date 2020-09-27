package com.brandon.order;

import com.brandon.flink.analysis.dto.OrderEvent;
import com.brandon.flink.analysis.dto.OrderResult;
import org.apache.flink.cep.PatternTimeoutFunction;

import java.util.List;
import java.util.Map;

public class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent, OrderResult> {
    @Override
    public OrderResult timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {

        OrderEvent create = pattern.get("create").get(0);

        return new OrderResult(create.orderId, "timeout:" + timeoutTimestamp);
    }
}
