package com.brandon.loginfail;

import com.brandon.flink.analysis.dto.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class LoginFailDetect {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> source = env.readTextFile("login-fail-detect/src/main/resources/LoginLog.csv");
        // 设置水位线
        DataStream<LoginEvent> dataStream = source.map(data -> {
            String[] arr = data.split(",");
            return new LoginEvent(Long.valueOf(arr[0]), arr[1], arr[2], Long.valueOf(arr[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
            @Override
            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                return element.timestamp * 1000;
            }
        }));

        // 判断检查，如果2秒之内连续登陆失败，输出报警信息
        dataStream.keyBy(new KeySelector<LoginEvent, Long>() {
            @Override
            public Long getKey(LoginEvent value) throws Exception {
                return value.userId;
            }
        }).process(new LoginFailWarningResult(2));


    }
}
