package com.brandon.order;

import com.brandon.flink.analysis.dto.LoginEvent;
import com.brandon.flink.analysis.dto.OrderEvent;
import com.brandon.flink.analysis.dto.OrderResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class OrderPayDetect {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> source = env.readTextFile("order-pay-detect/src/main/resources/OrderLog.csv");
        // 设置水位线
        DataStream<OrderEvent> dataStream = source.map(data -> {
            String[] arr = data.split(",");
            return new OrderEvent(Long.valueOf(arr[0]), arr[1],arr[2], Long.valueOf(arr[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                return element.timestamp * 1000;
            }
        })).keyBy(new KeySelector<OrderEvent, Long>() {
            @Override
            public Long getKey(OrderEvent value) throws Exception {
                return value.orderId;
            }
        });

        // 定义pattern, 15分钟内，先创建订单，再支付订单
        Pattern<OrderEvent, OrderEvent> orderPattern = Pattern.<OrderEvent>begin("create").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                return "create".equals(value.eventType);
            }
        }).followedBy("pay").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                return "pay".equals(value.eventType);
            }
        }).within(Time.minutes(15));


        // 应用到数据流上，进行模式匹配
        PatternStream<OrderEvent> patternStream = CEP.pattern(dataStream, orderPattern);

        // 定义侧输出流标签，用于处理超时事件
        OutputTag<OrderResult> orderTimeoutOutputTag = new OutputTag<OrderResult>("orderTimeout"){};

        //调用select方法，提取并处理匹配的成功支付事件及超时方法
        SingleOutputStreamOperator<OrderResult> orderResultStream = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect());

        orderResultStream.print("payed");
        orderResultStream.getSideOutput(orderTimeoutOutputTag).print("timeout");

        env.execute("order timeout job");




    }
}
