package com.brandon.order;

import com.brandon.flink.analysis.dto.OrderEvent;
import com.brandon.flink.analysis.dto.OrderResult;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import javax.security.auth.login.CredentialNotFoundException;
import java.time.Duration;

public class OrderTimeoutWithOutCEP {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);

//        DataStreamSource<String> source = env.readTextFile("order-pay-detect/src/main/resources/OrderLog.csv");
        // 设置水位线
        DataStream<OrderEvent> dataStream = source.map(data -> {
            String[] arr = data.split(",");
            return new OrderEvent(Long.valueOf(arr[0]), arr[1],arr[2], Long.valueOf(arr[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                return element.timestamp * 1000;
            }
        }));

        SingleOutputStreamOperator<OrderResult> orderResultStream = dataStream.keyBy(new KeySelector<OrderEvent, Long>() {
            @Override
            public Long getKey(OrderEvent value) throws Exception {
                return value.orderId;
            }
        }).process(new OrderProcess());

        orderResultStream.getSideOutput(new OutputTag<OrderResult>("orderTimeout"){}).print("timeout");

        orderResultStream.print();

        env.execute("order time");


    }
}
