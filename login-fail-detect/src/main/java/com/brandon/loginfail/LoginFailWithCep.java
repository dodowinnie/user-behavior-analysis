package com.brandon.loginfail;

import com.brandon.flink.analysis.dto.LoginEvent;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class LoginFailWithCep {

    public static void main(String[] args) throws Exception {
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

        // 使用CEP步骤
        // 1.定义匹配的模式，要求一个登录失败事件后紧跟另一个登录失败事件
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("fristFail").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                return "fail".equals(value.eventType);
            }
        }).next("secondFail").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                return "fail".equals(value.eventType);
            }
        }).within(Time.seconds(2));

        // 2.将模式应用到数据流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(dataStream.keyBy(new KeySelector<LoginEvent, Long>() {
            @Override
            public Long getKey(LoginEvent value) throws Exception {
                return value.userId;
            }
        }), loginFailPattern);

        // 3.检出符合模式的数据流，需要调用select方法
        patternStream.select(new LoginFailEventMatch()).print();

        env.execute("cep");
    }
}
