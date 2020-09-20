package com.brandon.flink.analysis.operate;

import com.brandon.flink.analysis.dto.ItemViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.*;

/**
 * Created by Brandoncui on 2020/9/19.
 */
public class TopNProcesFunction extends KeyedProcessFunction<Long, ItemViewCount, String> {

    private Integer topN;

    public TopNProcesFunction(Integer topN) {
        this.topN = topN;
    }

    // 定义状态
    private ListState<ItemViewCount> itemViewCountListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item_view_count_list", ItemViewCount.class));

    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        // 每来一条数据直接加入liststate
        itemViewCountListState.add(value);
        // 注册定时器,1ms触发
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);

    }

    //
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 为了方便排序，保存liststate所有数据
        List<ItemViewCount> allViewCounts = new ArrayList<ItemViewCount>();
        Iterator<ItemViewCount> iterator = itemViewCountListState.get().iterator();
        while (iterator.hasNext()){
            allViewCounts.add(iterator.next());
        }
        // 清空状态
        itemViewCountListState.clear();
        // 排序
        allViewCounts.sort((a, b)->Long.compare(b.getCount(), a.getCount()));
        List<ItemViewCount> topNCount = allViewCounts.subList(0, topN);
        // 格式化输出
        StringBuffer sb = new StringBuffer();
        sb.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
        // 遍历结果列表中的每个viewcount
        for(int i = 0; i < topNCount.size(); i++){
            ItemViewCount itemViewCount = topNCount.get(i);
            sb.append("NO").append(i + 1).append(":\t").append("商品ID：").append(itemViewCount.getItemId()).append("\t").append("热门度：").append(itemViewCount.getCount()).append("\n");
        }
        sb.append("========================\n\n");
        Thread.sleep(1000);
        out.collect(sb.toString());
    }


    public static void main(String[] args) {
        ItemViewCount i1 = new ItemViewCount(1l, null, (long) 1);
        ItemViewCount i2 = new ItemViewCount(4l, null, (long) 4);
        ItemViewCount i3 = new ItemViewCount(6l, null, (long) 6);
        ItemViewCount i4 = new ItemViewCount(2l, null, (long) 2);
        ItemViewCount i5 = new ItemViewCount(7l, null, (long) 7);
        ItemViewCount i6 = new ItemViewCount(9l, null, (long) 9);
        ItemViewCount i7 = new ItemViewCount(3l, null, (long) 3);
        List<ItemViewCount> items = new ArrayList<>();
        items.add(i1);
        items.add(i2);
        items.add(i3);
        items.add(i4);
        items.add(i5);
        items.add(i6);
        items.add(i7);

        System.out.println(items);
        items.sort((a, b) -> Long.compare(a.getCount(), b.getCount()));
        System.out.println("====================================================================");
        System.out.println(items);

    }
}
