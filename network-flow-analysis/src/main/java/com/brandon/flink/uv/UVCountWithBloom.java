package com.brandon.flink.uv;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 *
 */
public class UVCountWithBloom extends ProcessWindowFunction<Tuple2<String, Long>, UVCountResult, String, TimeWindow> {

    private Jedis jedis;

    private Bloom bloom;

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("127.0.0.1", 6379);
        bloom = new Bloom((1 << 29) * 1l); // 位的个数 2^6(64)*2^20(1M)*2^3(8bit)
    }

    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<UVCountResult> out) throws Exception {
        String sotreBitMapKey = context.window().getEnd() + "";
        // 将当前窗口的uv count值作为状态值保存到redis，用一个叫uvcount的hash表来保存(windowEnd, count)
        String uvCount = "uvcount";
        String currentKey = context.window().getEnd() + "";
        Long count = 0l;
        if (jedis.hget(uvCount, currentKey) != null) {
            count = Long.valueOf(jedis.hget(uvCount, currentKey));
        }
        // 去重
        Long userId = elements.iterator().next().f1;
        // 计算hash值，对应位图中偏移量
        Long offset = bloom.hash(userId.toString(), 61);
        Boolean isExist = jedis.getbit(sotreBitMapKey, offset);
        if (!isExist) {

            // 不存在， 位图置1，并将count+1
            jedis.setbit(sotreBitMapKey, offset, true);
            jedis.hset(uvCount, currentKey, (count + 1) + "");

        }

    }
}
