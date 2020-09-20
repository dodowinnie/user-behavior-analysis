package com.brandon.flink.analysis.again.TopN;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Created by Brandoncui on 2020/9/20.
 * 输入 UserBehavior
 * Acc Long
 * 输出 Long
 */
public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0l;
    }

    @Override
    public Long add(UserBehavior value, Long accumulator) {
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
