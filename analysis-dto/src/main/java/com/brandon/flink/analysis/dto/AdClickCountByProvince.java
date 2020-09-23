package com.brandon.flink.analysis.dto;

public class AdClickCountByProvince {

    public String windowEnd;

    public String province;

    public Long count;

    public AdClickCountByProvince(String windowEnd, String province, Long count) {
        this.windowEnd = windowEnd;
        this.province = province;
        this.count = count;
    }

    public AdClickCountByProvince() {
    }

    @Override
    public String toString() {
        return "AdClickCountByProvince{windowEnd=" + windowEnd + ", province=" + province + ",count=" + count + "}";
    }
}
