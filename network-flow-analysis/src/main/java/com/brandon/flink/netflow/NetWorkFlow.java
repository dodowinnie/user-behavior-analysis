package com.brandon.flink.netflow;

import com.brandon.flink.analysis.dto.ApacheLogEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Brandoncui on 2020/9/20.
 */
public class NetWorkFlow {


    /**
     * 每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL。
     * @param args
     * @throws ParseException
     */

    public static void main(final String[] args) throws ParseException {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        DataStreamSource<String> source = env.readTextFile("D:\\Idea\\user-behavior-analysis\\network-flow-analysis\\src\\main\\resources\\apache.log");
        // 抽取数据
        DataStream<ApacheLogEvent> dataStream = source.map(data -> {
            String[] arr = data.split(" ");
            return new ApacheLogEvent(arr[0], Long.valueOf(arr[2]), Long.valueOf(format.parse(arr[3]).getTime() / 1000), arr[5], arr[6]);
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<ApacheLogEvent>forMonotonousTimestamps().withTimestampAssigner((SerializableTimestampAssigner<ApacheLogEvent>) (element, recordTimestamp) -> element.eventTime * 1000));

        // 按url分组开窗,聚合
        dataStream.keyBy(new KeySelector<ApacheLogEvent, String>() {
            @Override
            public String getKey(ApacheLogEvent value) throws Exception {
                return value.url;
            }
        }).window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5))).aggregate(new CountAgg(), new WindowResultFunction());




    }



}
