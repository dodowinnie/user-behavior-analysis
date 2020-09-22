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

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
