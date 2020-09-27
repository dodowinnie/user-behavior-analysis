package com.brandon.loginfail;

import com.brandon.flink.analysis.dto.LoginEvent;
import com.brandon.flink.analysis.dto.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.ListValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LoginFailWarningResult extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

    private Integer failTimes;

    public LoginFailWarningResult(Integer failTimes) {
        this.failTimes = failTimes;
    }

    // 保存当前所有登陆失败事件
    private ListState<LoginEvent> loginFailListState;

    private ValueState<Long> timerTsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        loginFailListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
        timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsState", Long.class));
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
        if(timerTsState.value() == null){
            timerTsState.update(0l);
        }
        // 判断当前事件是成功还是失败
        if("fail".equals(value.eventType)){
            loginFailListState.add(value);
            if(timerTsState.value() == 0){
                Long ts = value.timestamp * 1000 + 2000;
                //注册定时器
                ctx.timerService().registerEventTimeTimer(ts);
                timerTsState.update(ts);
            }
        }else{
            // 清空定时器，状态列表
            ctx.timerService().deleteEventTimeTimer(timerTsState.value());
            timerTsState.clear();
            loginFailListState.clear();
        }
    }

    /**
     * 报警
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {

        List<LoginEvent> allLoginFailList = new ArrayList<>();
        Iterator<LoginEvent> iterator = loginFailListState.get().iterator();
        while (iterator.hasNext()){
            allLoginFailList.add(iterator.next());
        }
        // 判断登录失败事件的个数，如果超过上限，报警
        if(allLoginFailList.size() > failTimes){
            LoginEvent first = allLoginFailList.get(0);
            LoginEvent last = allLoginFailList.get(allLoginFailList.size() - 1);
             out.collect(new LoginFailWarning(first.userId, first.timestamp, last.timestamp, "login fail in 2s for " + allLoginFailList.size() + " times"));
        }
        // 清空状态
        loginFailListState.clear();
        timerTsState.clear();
    }
}
