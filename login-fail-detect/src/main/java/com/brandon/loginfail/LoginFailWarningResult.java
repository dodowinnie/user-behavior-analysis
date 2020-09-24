package com.brandon.loginfail;

import com.brandon.flink.analysis.dto.LoginEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.ListValue;
import org.apache.flink.util.Collector;

public class LoginFailWarningResult extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarningResult> {

    private Integer failTimes;

    public LoginFailWarningResult(Integer failTimes) {
        this.failTimes = failTimes;
    }

    // 保存当前所有登陆失败事件
    private ListState<LoginEvent> loginFailListState;

    private ValueState<Long> timerTsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        loginFailListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("loginfail-list", LoginEvent.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState", Long.class));
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarningResult> out) throws Exception {
        // 判断当前事件是成功还是失败



    }
}
