package com.brandon.flink.analysis.dto;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.StringUtils;

import java.util.Random;
import java.util.UUID;

/**
 * 自定义mapper，随机生成分组的key，防止数据倾斜
 */
public class PVMapper implements MapFunction<UserBehavior, Tuple2<String, Integer>> {
    public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
        String key = RandomStringUtils.randomAlphabetic(10);
        return new Tuple2<String, Integer>(key, 1);
    }
}
