package com.brandon.flink.analysis.dto;

/**
 * 访问
 */
public class ItemViewCount {

    public Long itemId;

    public Long windowEnd;

    public Long count;

    public Long getItemId() {
        return itemId;
    }

    public void setItemId(Long itemId) {
        this.itemId = itemId;
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

    public ItemViewCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public String toString() {
        return "ItemViewCount:{itemId:" + this.itemId + ",windowEnd:" + this.windowEnd + ",count:" + this.count + "}";
    }
}
