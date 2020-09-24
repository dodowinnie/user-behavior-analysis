package com.brandon.ad;

import com.brandon.flink.analysis.dto.AdClickLog;
import com.brandon.flink.analysis.dto.BlackListUserWarning;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FilterBlackListUserResult extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog> {


    private Long maxCount;

    public FilterBlackListUserResult(Long maxCount) {
        this.maxCount = maxCount;
    }



    // 定义状态，保存点击量
    private ValueState<Long> countState;
    // 每天零点定时清空状态的时间戳
    private ValueState<Long> resetTimeState;
    // 标记当前用户是否已经进入黑名单
    private ValueState<Boolean> isBlackState;


    @Override
    public void open(Configuration parameters) throws Exception {
        countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        resetTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("reset-ts", Long.class));
        isBlackState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-black", Boolean.class));

    }

    @Override
    public void processElement(AdClickLog value, Context ctx, Collector<AdClickLog> out) throws Exception {
        Long currentCount = countState.value() == null ? 0l : countState.value();
        if(isBlackState.value() == null){
            isBlackState.update(false);
        }

        // 判断只要是第一个数据来了，就注册0点清空的定时器
        if (currentCount == 0) {
            // 第二天0点的毫秒数
            Long ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000;
            resetTimeState.update(ts);
            ctx.timerService().registerProcessingTimeTimer(ts);
        }
        // 判断count值是否达到阈值，如果超过，则输出黑名单
        if(currentCount >= maxCount){
            // 判断是否已经在黑名单中，没有的话才输出
            if(!isBlackState.value()){
                isBlackState.update(true);
                // 输出侧输出流
                ctx.output(new OutputTag<BlackListUserWarning>("warning"){}, new BlackListUserWarning(value.userId, value.adId, "Click ad over" + maxCount + "times today"));
            }
            return;
        }

        // 正常情况，count+1,输出
        countState.update(currentCount + 1);
        out.collect(value);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickLog> out) throws Exception {
        // 定时清空
        if(timestamp == resetTimeState.value()){
            isBlackState.update(false);
            countState.update(0l);
        }
    }
}
