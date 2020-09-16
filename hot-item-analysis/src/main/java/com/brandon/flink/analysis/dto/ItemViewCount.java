package com.brandon.flink.analysis.dto;

/**
 * 访问
 */
public class ItemViewCount {

    public Long userId;

    public Long windowEnd;

    public Long count;


    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public ItemViewCount() {
    }

    public ItemViewCount(Long userId, Long windowEnd, Long count) {
        this.userId = userId;
        this.windowEnd = windowEnd;
        this.count = count;
    }
}
