package com.brandon.flink.netflow;

import com.brandon.flink.analysis.dto.ApacheLogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Created by Brandoncui on 2020/9/20.
 */
public class CountAgg implements AggregateFunction<ApacheLogEvent, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(ApacheLogEvent value, Integer accumulator) {
        return accumulator + 1;
    }

    @Override
    public Integer getResult(Integer accumulator) {
        return accumulator;
    }

    @Override
    public Integer merge(Integer a, Integer b) {
        return a + b;
    }
}
