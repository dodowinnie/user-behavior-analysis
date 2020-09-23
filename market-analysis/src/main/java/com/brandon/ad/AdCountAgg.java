package com.brandon.ad;

import com.brandon.flink.analysis.dto.AdClickLog;
import org.apache.flink.api.common.functions.AggregateFunction;

public class AdCountAgg implements AggregateFunction<AdClickLog, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0l;
    }

    @Override
    public Long add(AdClickLog value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
