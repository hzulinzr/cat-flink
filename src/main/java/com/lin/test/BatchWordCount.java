package com.lin.test;

import com.lin.entity.User;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lzr
 * @date 2020-09-03 13:17:04
 */
public class BatchWordCount {

//    批处理
//    public static void main(String[] args) throws Exception {
//        //加载flink的执行环境
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//
//        String path  = "/Users/lzr/study/cat-flink/src/main/java/com/lin/input/input.txt";
//        DataSource<String> stringDataSource = batchEnv.readTextFile(path);
//        AggregateOperator<Tuple2<String, Integer>> out = stringDataSource.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
//            String[] words = s.split(" ");
//            for (String word : words) {
//                collector.collect(new Tuple2<String, Integer>(word, 1));
//            }
//        }).returns(Types.TUPLE(Types.STRING, Types.INT))
//                //key，即（s, 1）中的s
//                .groupBy(0)
//                //value 即（s, 1）中的i
//                .sum(1);
//        out.print();
//
//    }

    //流处理
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> data = env.socketTextStream("localhost", 7777);
//        SingleOutputStreamOperator<Tuple2<String, Integer>> out = data.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, value) -> {
//            String[] words = s.split("\\s");
//            for (String word : words) {
//                value.collect(new Tuple2<>(word, 1));
//            }
//        })
//        .returns(Types.TUPLE(Types.STRING, Types.INT))
//                .keyBy(0)
//                .sum(1);
//        out.print();
        DataStreamSource<User> userDataStreamSource = env.fromElements(new User(1, "xiaohon", "xiaohei"),
                new User(2, "xiaohon", "xiaohei"),
                new User(3, "xiaohon", "xiaohei"));

        userDataStreamSource.print();


        env.execute();
    }

}
