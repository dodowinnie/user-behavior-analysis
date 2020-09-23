package com.brandon.ad;

import com.brandon.flink.analysis.dto.AdClickLog;
import com.brandon.flink.analysis.dto.BlackListUserWarning;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class FilterBlackListUserResult extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, BlackListUserWarning> {
    @Override
    public void processElement(AdClickLog value, Context ctx, Collector<BlackListUserWarning> out) throws Exception {

    }
}
