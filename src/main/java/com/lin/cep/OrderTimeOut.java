package com.lin.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author lzr
 * @date 2020-10-25 19:40:16
 */
public class OrderTimeOut {
    public static void main(String[] args) {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //读取数据
        KeyedStream<OrderEvent, Long> orderStream = env.readTextFile("/Users/lzr/study/cat-flink/src/main/resources/input/input.txt")
                .map(data -> {
                    String[] arr = data.split(",");
                    return new OrderEvent(Long.valueOf(arr[0].trim()), arr[1].trim(), arr[2].trim(), Long.valueOf(arr[3].trim()));
                }).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (orderEvent, l) -> orderEvent.getEventTime())
                )
                .keyBy(OrderEvent::getOrderId);

        //定义一个匹配模式
        Pattern<OrderEvent, OrderEvent> orderPayPattern = Pattern.<OrderEvent>begin("begin")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                        return "create".equals(orderEvent.getEventType());
                    }
                })
                .followedBy("follow")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                        return "pay".equals(orderEvent.getEventType());
                    }
                })
                .within(Time.seconds(15));

        //把模式运动到steam上
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderStream, orderPayPattern);

        //调用select方法，提取时间序列，超时事件做报警提示
        OutputTag<OrderResult> orderTimeOutTag = new OutputTag<OrderResult>("orderTimeOut") {
        };

        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select(orderTimeOutTag, new PatternTimeoutFunction<OrderEvent, OrderResult>() {
            @Override
            public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                long timeOutOrderId = map.get("begin").iterator().next().getOrderId();
                return new OrderResult(timeOutOrderId, "timeOut");
            }
        }, new PatternSelectFunction<OrderEvent, OrderResult>() {
            @Override
            public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
                long payOrderId = map.get("follow").iterator().next().getOrderId();
                return new OrderResult(payOrderId, "payed successfully");
            }
        });

        resultStream.print("pay");
        resultStream.getSideOutput(orderTimeOutTag).print("timeOut");

        try {
            env.execute("order time job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
