package com.lin.userbehavior;

import com.lin.vo.ItemViewCountVo;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author lzr
 * @date 2020-09-19 19:17:07
 * 基于用户行为分析
 */
public class UserBehaviorAnalysis {


    public static void main(String[] args) {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1)
                //设置时间语义为事件时间
                .setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        //获取相对路径
//        URL resource = UserBehaviorAnalysis.class.getResource("hdfs://cat01:9000/uerBehavior.txt");

        //读取数据
        SingleOutputStreamOperator<UserBehavior> dataStream = env.readTextFile("hdfs://cat01:9000/input/uerBehavior.txt")
                .map(date -> {
                    String[] dataArray = date.split(",");
                    //转换成想要的数据类型
                    return new UserBehavior(Long.valueOf(dataArray[0].trim()), Long.valueOf(dataArray[1].trim()),
                            Integer.parseInt(dataArray[2].trim()), dataArray[3].trim(), Long.valueOf(dataArray[4].trim()));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimeStamp();
                    }
                });



        //处理数据
        SingleOutputStreamOperator<String> process = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .keyBy(UserBehavior::getItemId)
                //滑动窗口
                .timeWindow(Time.hours(1), Time.minutes(5))
                //聚合处理
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy(ItemViewCountVo::getWindowsEnd)
                .process(new TopNHotItems(3));


        //输出
        process.print();


        System.out.println("================  hdfs ==================");
        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
                new Path("hdfs://cat01:9000/output"),
                new SimpleStringEncoder<String>("UTF-8")
        ).build();
        process.addSink(sink);

        try {
            env.execute("userBehavior job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
