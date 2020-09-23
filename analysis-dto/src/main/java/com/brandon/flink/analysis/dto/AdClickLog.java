package com.brandon.flink.analysis.dto;

public class AdClickLog {
    public Long userId;

    public Long adId;

    public String province;

    public String city;

    public Long timestamp;

    public AdClickLog(Long userId, Long adId, String province, String city, Long timestamp) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }

    public AdClickLog() {
    }
}
