package com.brandon.flink.analysis.operate;

import com.brandon.flink.analysis.dto.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Created by Brandoncui on 2020/9/16.
 * 聚合操作
 */
public class TopNFunction implements AggregateFunction<UserBehavior, Long, Long> {


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
