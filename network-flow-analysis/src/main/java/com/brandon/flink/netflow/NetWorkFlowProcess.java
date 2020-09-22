package com.brandon.flink.netflow;

import com.brandon.flink.analysis.dto.UrlViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NetWorkFlowProcess extends KeyedProcessFunction<Long, UrlViewCount, String> {

    private Integer topN;

    public NetWorkFlowProcess(Integer topN) {
        this.topN = topN;
    }

    // 定义状态列表
    private ListState<UrlViewCount> urlViewCountListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        urlViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<UrlViewCount>("url_count_list", UrlViewCount.class));
    }

    @Override
    public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
        // 添加到状态列表
        urlViewCountListState.add(value);
        // 设置定时器，相同key，相同时间的定时器只执行一次
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 从状态列表中拿出数据
        List<UrlViewCount> urls = new ArrayList<>();
        Iterator<UrlViewCount> iterator = urlViewCountListState.get().iterator();
        while (iterator.hasNext()) {
            urls.add(iterator.next());
        }
        // 清空状态列表
        urlViewCountListState.clear();
        // 排序
        urls.sort((x, y) -> Integer.compare(y.count, x.count));
        // 截取
        List<UrlViewCount> results = urls.subList(0, topN);
        // 输出
        StringBuffer sb = new StringBuffer();
        sb.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < results.size(); i++) {
            UrlViewCount urlViewCount = results.get(i);
            sb.append("NO").append(i + 1).append("\t").append("url为：").append(urlViewCount.url).append("\t").append("热度为：").append(urlViewCount.count).append("\n");
        }
        sb.append("*********************************\n\n");
        Thread.sleep(1000);
        out.collect(sb.toString());
    }
}
