package com.brandon.flink.pageview;

import com.brandon.flink.analysis.dto.PageViewCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PVWindowFunction implements WindowFunction<Integer, PageViewCount, String, TimeWindow> {
    @Override
    public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<PageViewCount> out) throws Exception {
        out.collect(new PageViewCount(window.getEnd(), input.iterator().next()));
    }
}
