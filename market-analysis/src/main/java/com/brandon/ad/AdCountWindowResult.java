package com.brandon.ad;

import com.brandon.flink.analysis.dto.AdClickCountByProvince;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AdCountWindowResult implements WindowFunction<Long, AdClickCountByProvince, String, TimeWindow> {

    @Override
    public void apply(String s, TimeWindow window, Iterable<Long> input, Collector<AdClickCountByProvince> out) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long end = window.getEnd();
        out.collect(new AdClickCountByProvince(format.format(new Date(end)), s, input.iterator().next()));
    }
}
