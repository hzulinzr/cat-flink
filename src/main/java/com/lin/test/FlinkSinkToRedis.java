package com.lin.test;

import com.lin.entity.User;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;

/**
 * @author lzr
 * 输出数据到redis中
 * @date 2020-09-10 15:38:43
 */
public class FlinkSinkToRedis {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> data = env.readTextFile("/Users/lzr/study/cat-flink/src/main/java/com/lin/input/input.txt");
        SingleOutputStreamOperator<User> map = data.map(in -> {
            String[] strArray = in.split(",");
            return new User(Long.parseLong(strArray[0]), strArray[1], strArray[2]);
        });

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();

        map.addSink(new RedisSink<>(conf, new MyRedisSinkMapper()));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    static class MyTimeMap extends RichFlatMapFunction<User, String>{

        private ValueState<Double> time;


        @Override
        public void open(Configuration parameters){
            time = getRuntimeContext().getState(new ValueStateDescriptor<>("time", Double.class));
        }

        @Override
        public void flatMap(User user, Collector<String> collector) throws Exception {

        }
    }
}
