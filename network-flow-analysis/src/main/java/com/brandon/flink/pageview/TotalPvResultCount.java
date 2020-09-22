package com.brandon.flink.pageview;

import com.brandon.flink.analysis.dto.PageViewCount;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TotalPvResultCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {

    private ValueState<Integer> total;

    @Override
    public void open(Configuration parameters) throws Exception {
        total = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("total", Integer.class));

    }

    @Override
    public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
        Integer v = total.value() == null ? 0 : total.value();
        total.update(v + value.count);
        // 注册定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
        out.collect(new PageViewCount(ctx.getCurrentKey(), total.value()));
        total.clear();
    }
}
