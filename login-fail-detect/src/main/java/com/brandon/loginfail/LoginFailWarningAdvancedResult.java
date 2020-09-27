package com.brandon.loginfail;

import com.brandon.flink.analysis.dto.LoginEvent;
import com.brandon.flink.analysis.dto.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class LoginFailWarningAdvancedResult extends KeyedProcessFunction<Long, LoginEvent, LoginFailWarning> {

    private Integer failTimes;

    public LoginFailWarningAdvancedResult(Integer failTimes) {
        this.failTimes = failTimes;
    }

    // 保存当前所有登陆失败事件
    private ListState<LoginEvent> loginFailListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        loginFailListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list", LoginEvent.class));
    }

    @Override
    public void processElement(LoginEvent value, Context ctx, Collector<LoginFailWarning> out) throws Exception {
        if("fail".equals(value.eventType)){
            Iterator<LoginEvent> iterator = loginFailListState.get().iterator();
            if (iterator.hasNext()){
                LoginEvent lastEvent = iterator.next();
                if((value.timestamp - lastEvent.timestamp) < 2){
                    // 输出警报
                    out.collect(new LoginFailWarning(lastEvent.userId, lastEvent.timestamp, value.timestamp, "login fail 2 times in 2s " ));
                }
            }else{
                loginFailListState.add(value);
            }
            // 更新为最近一次登录失败的事件
            loginFailListState.clear();
            loginFailListState.add(value);
        }else{
            // 清空状态
            loginFailListState.clear();
        }

    }


}
