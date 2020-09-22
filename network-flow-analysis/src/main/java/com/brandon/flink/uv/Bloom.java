package com.brandon.flink.uv;

import java.io.Serializable;

/**
 * 自定义bloom过滤器，主要是一个位图和hash函数
 */
public class Bloom implements Serializable {

    // 位图容量
    private Long cap; // 默认是2的整次幂

    public Bloom(Long cap) {
        this.cap = cap;
    }

    public Long getCap() {
        return cap;
    }

    public void setCap(Long cap) {
        this.cap = cap;
    }

    public Long hash(String userId, Integer seed){
        Long result = 0l;
        for(int i = 0; i < userId.length(); i++){
            result = result * seed + userId.charAt(i);
        }
        // 返回hash值，要映射到cap范围内
        result = (cap - 1) & result;
        return result;
    }

}
