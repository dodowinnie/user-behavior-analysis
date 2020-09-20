package com.brandon.flink.analysis.operate;

import com.brandon.flink.analysis.dto.ItemViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by Brandoncui on 2020/9/16.
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Long, TimeWindow> {

    @Override
    public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        out.collect(new ItemViewCount(aLong,window.getEnd(),input.iterator().next()));

    }
}
