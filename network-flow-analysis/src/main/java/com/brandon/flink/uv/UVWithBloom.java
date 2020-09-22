package com.brandon.flink.uv;

import com.brandon.flink.analysis.dto.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class UVWithBloom {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.readTextFile("network-flow-analysis/src/main/resources/UserBehavior.csv");
        DataStream<UserBehavior> dataStream = source.map(data -> {
            String[] arr = data.split(",");
            return new UserBehavior(Long.valueOf(arr[0]), Long.valueOf(arr[1]), Integer.valueOf(arr[2]), arr[3], Long.valueOf(arr[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.timestamp * 1000;
            }
        }));

        dataStream
                .filter(x -> "pv".equals(x.behavior))
                .map(data -> new Tuple2<String, Long>("uv", data.userId)).returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
        })).keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                }).timeWindow(Time.hours(1))
                .trigger(new MyTrigger()).process(new UVCountWithBloom());
        env.execute("uv");

    }
}
