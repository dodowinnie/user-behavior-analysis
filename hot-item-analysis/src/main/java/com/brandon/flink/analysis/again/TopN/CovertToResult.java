package com.brandon.flink.analysis.again.TopN;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by Brandoncui on 2020/9/20.
 */
public class CovertToResult implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {
    @Override
    public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        Long itemId = aLong;
        Long windowEnd = window.getEnd();
        Long count = input.iterator().next();
        out.collect(new ItemViewCount(aLong,windowEnd,count));

    }
}
