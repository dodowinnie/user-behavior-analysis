package com.brandon.loginfail;

import com.brandon.flink.analysis.dto.LoginEvent;
import com.brandon.flink.analysis.dto.LoginFailWarning;
import org.apache.flink.cep.PatternSelectFunction;

import java.util.List;
import java.util.Map;

public class LoginFailEventMatch implements PatternSelectFunction<LoginEvent, LoginFailWarning> {
    @Override
    public LoginFailWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
        // 当前匹配到事件序列，保存到pattern里
        LoginEvent fristFail = pattern.get("fristFail").get(0);
        LoginEvent secondFail = pattern.get("secondFail").get(0);
    return new LoginFailWarning(fristFail.userId, fristFail.timestamp, secondFail.timestamp, "login fail 2 times");
    }
}
