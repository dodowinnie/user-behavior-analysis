package com.brandon.flink.analysis.dto;

public class MarketingUserBehavior {

    public String userId;

    public String behavior;

    public String channel;

    public Long timestamp;

    public MarketingUserBehavior(String userId, String behavior, String channel, Long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }

    public MarketingUserBehavior() {
    }

}
