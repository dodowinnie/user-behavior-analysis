package com.brandon.flink.analysis.dto;

public class UniqueViewCount {

    public Long windowEnd;

    public Integer count;

    public UniqueViewCount(Long windowEnd, Integer count) {
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public UniqueViewCount() {
    }

    @Override
    public String toString() {
        return "UV(" + windowEnd + ", " + count + ")";
    }
}
