package com.brandon.flink.analysis.again.TopN;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Brandoncui on 2020/9/20.
 */
public class TopProcess extends KeyedProcessFunction<Long, ItemViewCount, String> {

    // 定义top n
    private Integer topn;

    public TopProcess(Integer topn) {
        this.topn = topn;
    }

    // 定义列表状态，存储根据key排序后放进来的itemViewCount
    private ListState<ItemViewCount> itemViewCountListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化
         itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item_view_count", ItemViewCount.class));
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        itemViewCountListState.add(value);
        // 定义触发器，1ms后执行
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }


    // 定义相同的定时器仅仅会执行一次
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 将listState中的数据取出放到list中
        List<ItemViewCount> list = new ArrayList<>();
        Iterator<ItemViewCount> iterator = itemViewCountListState.get().iterator();
        while (iterator.hasNext()){
            list.add(iterator.next());
        }
        // TODO 清除
        itemViewCountListState.clear();
        // list排序
        list.sort((a,b)->Long.compare(b.getCount(), a.getCount()));
        // 抽取
        List<ItemViewCount> result = list.subList(0, topn);
        // 输出
        StringBuffer sb = new StringBuffer();
        sb.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < result.size(); i++){
            ItemViewCount itemViewCount = result.get(i);
            sb.append("NO").append((i+1)).append("\t").append("itemID:").append(itemViewCount.getItemId()).append("\t").append("热度：").append(itemViewCount.getCount()).append("\n");
        }
        sb.append("================================\n\n");
        Thread.sleep(1000);
        out.collect(sb.toString());

    }
}
