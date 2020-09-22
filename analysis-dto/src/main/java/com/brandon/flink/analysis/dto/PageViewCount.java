package com.brandon.flink.analysis.dto;

public class PageViewCount {

    public Long windowEnd;

    public Integer count;

    public PageViewCount(Long windowEnd, Integer count) {
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public PageViewCount() {
    }

    @Override
    public String toString() {
        return "PV(" + windowEnd + ", " + count + ")";
    }
}
