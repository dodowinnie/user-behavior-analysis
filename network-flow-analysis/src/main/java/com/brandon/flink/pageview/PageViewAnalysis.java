package com.brandon.flink.pageview;

import com.brandon.flink.analysis.dto.PVMapper;
import com.brandon.flink.analysis.dto.PageViewCount;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 实时统计每小时内的网站 PV
 */
public class PageViewAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.readTextFile("network-flow-analysis/src/main/resources/UserBehavior.csv");
        // 转换数据，设置水位线
        DataStream<UserBehavior> dataStream = source.map(data -> {
            String[] arr = data.split(",");
            return new UserBehavior(Long.valueOf(arr[0]), Long.valueOf(arr[1]), Integer.valueOf(arr[2]), arr[3], Long.valueOf(arr[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.timestamp * 1000;
            }
        }));

        // PV过滤开窗统计
//        dataStream.filter(x->"pv".equals(x.behavior)).keyBy(new KeySelector<UserBehavior, String>() {
//            @Override
//            public String getKey(UserBehavior value) throws Exception {
//                return value.getBehavior();
//            }
//        }).window(TumblingEventTimeWindows.of(Time.hours(1))).aggregate(new PVSum(), new PVWindowFunction()).print();
        // PV过滤开窗统计,防止数据倾斜
        DataStream<Tuple2<String, Integer>> map = dataStream.filter(x -> "pv".equals(x.behavior)).map(new PVMapper());

        DataStream<PageViewCount> count = map.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).window(TumblingEventTimeWindows.of(Time.hours(1))).aggregate(new PVSumV2(), new PVWindowFunctionV2()).keyBy(new KeySelector<PageViewCount, Long>() {
            @Override
            public Long getKey(PageViewCount value) throws Exception {
                return value.windowEnd;
            }
        }).process(new TotalPvResultCount());
        count.print();


        env.execute("per-hour pv sum");


    }
}
