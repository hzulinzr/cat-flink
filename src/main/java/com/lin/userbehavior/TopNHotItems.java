package com.lin.userbehavior;

import com.lin.cep.OrderResult;
import com.lin.entity.User;
import com.lin.vo.ItemViewCountVo;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lzr
 * @date 2020-09-22 21:26:50
 */
public class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCountVo, String> {

    private ListState<ItemViewCountVo> itemStates;


    private Integer topN;

    public TopNHotItems(int topN) {
        this.topN = topN;
    }

    @Override
    public void open(Configuration parameters){
        itemStates = getRuntimeContext().getListState(new ListStateDescriptor<>("item-state", ItemViewCountVo.class));
    }

    @Override
    public void processElement(ItemViewCountVo itemViewCountVo, Context context, Collector<String> collector) throws Exception {

        //把每条数据存入状态列表中
        itemStates.add(itemViewCountVo);

        //注册一个定时器
        context.timerService().registerEventTimeTimer(itemViewCountVo.getWindowsEnd() + 1);
    }

    /**
     * 定时器触发时，对所有数据排序，并输出结果
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<ItemViewCountVo> allItems = new ArrayList<>();
        List<ItemViewCountVo> topList = new ArrayList<>();

        for(ItemViewCountVo item : itemStates.get()){
            allItems.add(item);
        }

        //排序
        allItems.sort( (o1, o2) -> o2.getCount().compareTo(o1.getCount()));

        //取前n条数
        if(topN > allItems.size() ){
            topN = allItems.size();
        }
        topList = allItems.subList(0, topN);

        //清空状态
        itemStates.clear();

        //将排名结果格式化输出
        StringBuilder result = new StringBuilder();


        result.append("时间：").append(new Timestamp((timestamp-1))).append("\n");

        //输出每一个商品信息
        for(int i = 0; i < topList.size(); i++){
            result.append("No.").append(i + 1)
                    .append(" 商品ID=").append(topList.get(i).getItemId())
                    .append(" 浏览量=").append(topList.get(i).getCount())
                    .append("\n");
        }

        result.append("===========================");

        //控制输出频率
        Thread.sleep(1000);
        out.collect(result.toString());

    }


}
