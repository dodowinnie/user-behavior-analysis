package com.brandon.flink.pageview;

import com.brandon.flink.analysis.dto.PageViewCount;
import com.brandon.flink.analysis.dto.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

public class PVSum implements AggregateFunction<UserBehavior, Integer, Integer> {
    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(UserBehavior value, Integer accumulator) {
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
