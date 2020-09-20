package com.brandon.flink.analysis.dto;

/**
 * Created by Brandoncui on 2020/9/20.
 */
public class ApacheLogEvent {

    public String ip;

    public Long userId;

    public Long eventTime;

    public String method;

    public String url;

    public ApacheLogEvent(String ip, Long userId, Long eventTime, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    public ApacheLogEvent() {
    }

    @Override
    public String toString() {
        return "ip: " + this.ip + ", userID: " + this.userId + ", eventTime: " + this.eventTime + ", method: " + this
                .method + ", url: " + this.url;
    }
}
