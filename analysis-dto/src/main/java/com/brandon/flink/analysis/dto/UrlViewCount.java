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

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }
}
