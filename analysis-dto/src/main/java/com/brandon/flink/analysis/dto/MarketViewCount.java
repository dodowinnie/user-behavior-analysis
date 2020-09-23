package com.brandon.flink.analysis.dto;

public class MarketViewCount {

    public String windowStart;

    public String windowEnd;

    public String channel;

    public String behavior;

    public Long count;

    public MarketViewCount(String windowStart, String windowEnd, String channel, String behavior, Long count) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.channel = channel;
        this.behavior = behavior;
        this.count = count;
    }

    public MarketViewCount() {
    }

    @Override
    public String toString() {
        return "MarketViewCount{windowStart = " + windowStart + ",windowEnd = " + windowEnd + ",channel = " + channel + ",behavior = " + behavior + ",count = " + count + "}";
    }
}
