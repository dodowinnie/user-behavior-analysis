package com.brandon.flink.analysis.dto;

/**
 * 侧输出流信息
 */
public class BlackListUserWarning {

    public Long userId;

    public Long adId;

    public String msg;

    public BlackListUserWarning(Long userId, Long adId, String msg) {
        this.userId = userId;
        this.adId = adId;
        this.msg = msg;
    }

    public BlackListUserWarning() {
    }
}
