package com.brandon.ad;

import com.brandon.flink.analysis.dto.AdClickLog;
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

public class AdClickAnalysis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.readTextFile("market-analysis/src/main/resources/AdClickLog.csv");

        DataStream<AdClickLog> dataStream = source.map(data -> {
            String[] arr = data.split(",");
            return new AdClickLog(Long.valueOf(arr[0]), Long.valueOf(arr[1]), arr[2], arr[3], Long.valueOf(arr[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<AdClickLog>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<AdClickLog>() {
            @Override
            public long extractTimestamp(AdClickLog element, long recordTimestamp) {
                return element.timestamp * 1000;
            }
        }));

        // 插入过滤操作，并将有刷单行为的用户，输出到侧输出流（黑名单报警）
        dataStream.keyBy(new KeySelector<AdClickLog, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> getKey(AdClickLog value) throws Exception {
                return new Tuple2<Long, Long>(value.userId, value.adId);
            }
        }).process(new FilterBlackListUserResult());



        dataStream.keyBy(new KeySelector<AdClickLog, String>() {
            @Override
            public String getKey(AdClickLog value) throws Exception {
                return value.province;
            }
        }).timeWindow(Time.days(1), Time.seconds(5)).aggregate(new AdCountAgg(), new AdCountWindowResult()).print();
        env.execute("ad count");

    }
}
