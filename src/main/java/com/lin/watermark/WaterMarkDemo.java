package com.lin.watermark;

import com.lin.cep.OrderEvent;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author lzr
 * @date 2020-12-08 23:52:09
 */
public class WaterMarkDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1).setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> inputData = env.readTextFile("/Users/lzr/study/cat-flink/src/main/resources/input/input.txt");

        inputData.map(in -> {
            String[] datas = in.split(",");
            return new OrderEvent(Long.parseLong(datas[0].trim()), datas[1].trim(), datas[2].trim(), Long.parseLong(datas[3].trim()));
        })//自定义watermark生成策略
                .assignTimestampsAndWatermarks(new WatermarkStrategy<OrderEvent>() {
            @Override
            public WatermarkGenerator<OrderEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<OrderEvent>() {
                    private long maxTimestamp;
                    private final long delay = 3000;
                    @Override
                    public void onEvent(OrderEvent event, long eventTimestamp, WatermarkOutput output) {
                        maxTimestamp = Math.max(event.getEventTime(), maxTimestamp);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimestamp - delay));
                    }
                };
            }
        });
//                调用flink自带的watermark生成策略
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                //指定延迟时间为5s
//                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                .withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>(){
//                    @Override
//                    public long extractTimestamp(OrderEvent orderEvent, long recordTimeStamp) {
//                        //指定时间时间
//                        return orderEvent.getEventTime();
//                    }
//                })
//
//        );


    }
}
