package com.brandon.flink.analysis.dto;

/**
 * Created by Brandoncui on 2020/9/20.
 */
public class UrlViewCount {

    public String url;

    public Integer count;

    public Long windowEnd;

    public UrlViewCount(String url, Integer count, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowEnd = windowEnd;
    }

    public UrlViewCount() {
    }
}
