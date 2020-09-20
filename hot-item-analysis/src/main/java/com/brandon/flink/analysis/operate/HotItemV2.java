package com.brandon.flink.analysis.operate;

import com.brandon.flink.analysis.dto.ItemViewCount;
import com.brandon.flink.analysis.dto.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 实时热门商品，每隔5分钟输出一次点击量topN数据
 */
public class HotItemV2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 读取数据
        DataStreamSource<String> stream = env.readTextFile("D:\\Idea\\user-behavior-analysis\\hot-item-analysis\\src\\main\\resources\\UserBehavior.csv");
        // 转换数据
        DataStream<UserBehavior> dataStream = stream.map(data -> {
            String[] arr = data.split(",");
            // 543462,1715,1464116,pv,1511658000
            return new UserBehavior(Long.valueOf(arr[0]), Long.valueOf(arr[1]), Integer.valueOf(arr[2]), arr[3], Long.valueOf(arr[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp() * 1000;
            }
        }));
        // 得到窗口聚合结果
        DataStream<ItemViewCount> aggregateStream = dataStream
                .filter(x -> "pv".equals(x.behavior)) // 过滤行为
                .keyBy(new KeySelector<UserBehavior, Long>() { // 按项目分组
                    @Override
                    public Long getKey(UserBehavior value) throws Exception {
                        return value.getItemId();
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.hours(1l), Time.minutes(5l))) // 滑动窗口分组
                .aggregate(new TopNFunction(), new WindowResultFunction());

        // 得到窗口结果
        aggregateStream.keyBy(new KeySelector<ItemViewCount, Long>() {
            @Override
            public Long getKey(ItemViewCount value) throws Exception {
                return value.getWindowEnd();
            }
        }).process(new TopNProcesFunction(5)).print();


        env.execute("select top n");


    }
}
