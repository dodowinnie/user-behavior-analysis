package com.brandon.market;

import com.brandon.flink.analysis.dto.MarketViewCount;
import com.brandon.flink.analysis.dto.MarketingUserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

public class MyMarketChannelCount extends ProcessWindowFunction<MarketingUserBehavior, MarketViewCount, Tuple2<String, String>, TimeWindow> {
    @Override
    public void process(Tuple2<String, String> key, Context context, Iterable<MarketingUserBehavior> elements, Collector<MarketViewCount> out) throws Exception {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = context.window().getStart();
        String startStr = format.format(new Date(start));
        long end = context.window().getEnd();
        String endStr = format.format(new Date(end));
        String channel = key.f0;
        String behavior =key.f1;
        Long count = 0l;
        Iterator<MarketingUserBehavior> iterator = elements.iterator();

        while (iterator.hasNext()){
            MarketingUserBehavior ele = iterator.next();
            count += 1;
        }
        MarketViewCount marketViewCount = new MarketViewCount(startStr, endStr, channel, behavior, count);
        out.collect(marketViewCount);

    }
}
