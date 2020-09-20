package com.brandon.flink.analysis.again.TopN;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by Brandoncui on 2020/9/20.
 */
public class TopNMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 读取数据
        DataStreamSource<String> stream = env.readTextFile("D:\\Idea\\user-behavior-analysis\\hot-item-analysis\\src\\main\\resources\\UserBehavior.csv");
        DataStream<UserBehavior> datastream = stream.map(data -> {
            String[] arr = data.split(",");
            // 95305,2229897,1789614,pv,1511658336
            return new UserBehavior(Long.valueOf(arr[0]), Long.valueOf(arr[1]), Integer.valueOf(arr[2]), arr[3], Long.valueOf(arr[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<UserBehavior>) (element, recordTimestamp) -> element.getTimestamp() * 1000));
        // 过滤分组,聚合, 该窗口结束时，每个item的pv数
        DataStream<ItemViewCount> aggregateStream = datastream
                .filter(x -> "pv".equals(x.getBehavior()))
                .keyBy((KeySelector<UserBehavior, Long>) value -> value.getItemId())
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(new CountAgg(), new CovertToResult());
        // 以windowend分组，进行排序，输出
        DataStream<String> resultStream = aggregateStream
                .keyBy((KeySelector<ItemViewCount, Long>) x -> x.getWindowEnd())
                .process(new TopProcess(5));

        resultStream.print();
        env.execute("sort");


    }
}
