package com.brandon.order;

import com.brandon.flink.analysis.dto.OrderEvent;
import com.brandon.flink.analysis.dto.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OrderProcess extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {


    private ValueState<Boolean> isPayArrive;

    private ValueState<Boolean> isCreateArrive;

    private ValueState<Long> timeState;

    private OutputTag<OrderResult> orderResultOutputTag;

    @Override
    public void open(Configuration parameters) throws Exception {
        isPayArrive = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isPayArrive", Boolean.class));
        isCreateArrive = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("isCreateArrive", Boolean.class));
        timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState", Long.class));
        orderResultOutputTag = new OutputTag<OrderResult>("timeout"){};

    }

    @Override
    public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {
        if(isPayArrive == null){
            isPayArrive.update(false);
        }
        if(isCreateArrive == null){
            isCreateArrive.update(false);
        }

        if("create".equals(value.eventType)){
            if(isPayArrive.value()){
                // 已支付, 正常支付，输出匹配成功结果
                out.collect(new OrderResult(value.orderId, "payed successfully"));
                // 清空定时器和状态
                ctx.timerService().deleteEventTimeTimer(timeState.value());
                isPayArrive.clear();
                isCreateArrive.clear();
                timeState.clear();
            }else{
                // 没有支付
                isCreateArrive.update(true);
                // 注册定时器
                long ts = value.timestamp * 1000 + 15 * 60 * 1000;
                ctx.timerService().registerEventTimeTimer(ts);
                timeState.update(ts);
            }
        }


        if("pay".equals(value.eventType)){
            if(isCreateArrive.value()){
                if(value.timestamp * 1000 < timeState.value()){
                    // 没有超时，正常输出
                    // 已创订单,正常支付，输出匹配成功结果
                    out.collect(new OrderResult(value.orderId, "payed successfully"));
                }else{
                    // 超时，侧输出流,可能业务系统出问题
                    ctx.output(orderResultOutputTag, new OrderResult(value.orderId, "payed but already timeout"));
                }
                // 清空状态
                // 清空定时器和状态
                ctx.timerService().deleteEventTimeTimer(timeState.value());
                isPayArrive.clear();
                isCreateArrive.clear();
                timeState.clear();
            }else{
                // 没有create，注册定时器，等到pay的时间就可以
                isPayArrive.update(true);
                ctx.timerService().registerEventTimeTimer(value.timestamp * 1000);
                timeState.update(value.timestamp * 1000);
            }
        }
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
        // pay来了，create没来
        if(isPayArrive.value() && !isCreateArrive.value()){
            ctx.output(orderResultOutputTag, new OrderResult(Long.valueOf(ctx.getCurrentKey()),"pay arrive but create is not arrive"));
        }

        // create来了，pay没来
        if(!isPayArrive.value() && isCreateArrive.value()){
            ctx.output(orderResultOutputTag, new OrderResult(Long.valueOf(ctx.getCurrentKey()),"create arrive but pay is not arrive"));
        }
        // 清空状态
        ctx.timerService().deleteEventTimeTimer(timestamp);
        isCreateArrive.clear();
        isPayArrive.clear();
        timeState.clear();
    }
}
