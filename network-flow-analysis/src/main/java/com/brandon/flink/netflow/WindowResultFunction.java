package com.brandon.flink.netflow;

import com.brandon.flink.analysis.dto.UrlViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created by Brandoncui on 2020/9/20.
 */
public class WindowResultFunction implements WindowFunction<Integer,UrlViewCount,String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<UrlViewCount> out) throws Exception {
        String url = s;
        Long windowEnd = window.getEnd();
        Integer count = input.iterator().next();
        out.collect(new UrlViewCount(s,count,windowEnd));
    }
}
