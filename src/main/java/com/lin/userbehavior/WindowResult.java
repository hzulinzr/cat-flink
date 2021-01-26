package com.lin.userbehavior;
import com.lin.vo.ItemViewCountVo;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
/**
 * @author lzr
 * @date 2020-09-20 14:34:48
 * 自定义窗口函数， 输出itemViewCount
 */
public class WindowResult implements WindowFunction<Long, ItemViewCountVo, Long, TimeWindow> {

    @Override
    public void apply(Long key, TimeWindow timeWindow, Iterable<Long> input, Collector<ItemViewCountVo> collector) {
        collector.collect(new ItemViewCountVo(key, timeWindow.getEnd(), input.iterator().next()));
    }
}