package com.brandon.flink.uv;

import com.brandon.flink.analysis.dto.UniqueViewCount;
import com.brandon.flink.analysis.dto.UserBehavior;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

// 自定义全窗口函数，用一个set保存数据（userID），进行自动去重
public class UVCountResult implements AllWindowFunction<UserBehavior, UniqueViewCount, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<UniqueViewCount> out) throws Exception {
        Set<Long> userIdSet = new HashSet<Long>();
        Iterator<UserBehavior> iterator = values.iterator();
        while (iterator.hasNext()){
            userIdSet.add(iterator.next().userId);
        }
        out.collect(new UniqueViewCount(window.getEnd(), userIdSet.size()));
    }
}
