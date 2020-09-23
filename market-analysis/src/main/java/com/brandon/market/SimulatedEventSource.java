package com.brandon.market;

import com.brandon.flink.analysis.dto.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * 自定义数据源
 */
public class SimulatedEventSource extends RichParallelSourceFunction<MarketingUserBehavior> {

    private Boolean running = true;

    private List<String> channelList = Arrays.asList("AppStore", "XiaomiStore", "HuaweiStore", "weibo",
            "wechat", "tieba");

    private List<String> behaviorTypes = Arrays.asList("BROWSE", "CLICK", "PURCHASE", "UNINSTALL");

    private Random random = new Random();

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        Long maxElements = Long.MAX_VALUE;
        long count = 0l;

        while (running && count < maxElements){
            String userId = UUID.randomUUID().toString();
            String behaviorType = behaviorTypes.get(random.nextInt(behaviorTypes.size()));
            String channel = channelList.get(random.nextInt(channelList.size()));
            Long timestamp = System.currentTimeMillis();
            ctx.collectWithTimestamp(new MarketingUserBehavior(userId, behaviorType, channel, timestamp), timestamp);
            count += 1;
            Thread.sleep(5);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }

    public static void main(String[] args) {
        Random random = new Random();
        System.out.println(random.nextLong());
    }
}
