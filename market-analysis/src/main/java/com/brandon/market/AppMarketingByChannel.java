package com.brandon.market;

import com.brandon.flink.analysis.dto.MarketingUserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 分渠道统计
 */
public class AppMarketingByChannel {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置水位线
        DataStream<MarketingUserBehavior> dataStream = env.addSource(new SimulatedEventSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<MarketingUserBehavior>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<MarketingUserBehavior>) (element, recordTimestamp) -> element.timestamp));
        // 开窗，聚合
        dataStream.filter(x->!"UNINSTALL".equals(x.behavior)).keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(MarketingUserBehavior value) throws Exception {
                return new Tuple2<String, String>(value.channel, value.behavior);
            }
        }).timeWindow(Time.days(1), Time.seconds(5)).process(new MyMarketChannelCount()).print();
        env.execute("market");

    }
}
