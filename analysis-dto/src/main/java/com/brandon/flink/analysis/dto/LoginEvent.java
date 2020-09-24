package com.brandon.flink.analysis.dto;

public class LoginEvent {

    public Long userId;

    public String ip;

    public String eventType;

    public Long timestamp;

    public LoginEvent(Long userId, String ip, String eventType, Long timestamp) {
        this.userId = userId;
        this.ip = ip;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }



    public LoginEvent() {
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId=" + userId +
                ", ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
