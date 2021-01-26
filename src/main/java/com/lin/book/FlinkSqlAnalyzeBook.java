package com.lin.book;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author lzr
 * @date 2021-01-21 18:54:08
 */
public class FlinkSqlAnalyzeBook {
    public static void main(String[] args) {
        //构造流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //构造table环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        //构造hive的配置
        String name = "hive";
        String database = "hive_test";
        String hiveConf = "/usr/local/Cellar/hive/3.1.2/libexec/conf";

        Catalog catalog = new HiveCatalog(name, database, hiveConf);
        tableEnv.registerCatalog("hive", catalog);
        tableEnv.useCatalog("hive");
        tableEnv.useDatabase("hive_test");

        // 构造用户行为表
        tableEnv.executeSql("DROP TABLE IF EXISTS user_behavior");
        tableEnv.executeSql("CREATE TABLE user_behavior (\n" +
                "    user_id BIGINT,\n" +
                "    book_id BIGINT,\n" +
                "    book_type_id BIGINT,\n" +
                "    behavior STRING,\n" +
                "    create_time BIGINT,\n" +
                "    proctime AS PROCTIME(),\n" +
                "    ts AS TO_TIMESTAMP(FROM_UNIXTIME(create_time / 1000, 'yyyy-MM-dd HH:mm:ss'))," +
                "    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'user_behavior',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'format' = 'json'\n" +
                ")");


        //        构造es表来表示每小时用户的点赞数
        tableEnv.executeSql("DROP TABLE IF EXISTS book_thumbs_per_hour");
        tableEnv.executeSql("CREATE TABLE book_thumbs_per_hour (\n" +
                "    hour_of_day BIGINT,\n" +
                "    book_thumbs BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'elasticsearch-7', \n" +
                "    'hosts' = 'http://localhost:9200', \n" +
                "    'sink.bulk-flush.max-actions' = '1', \n" +
                "    'index' = 'book_thumbs_per_hour' \n" +
                ")");


        // 数据写入，无需提交，使用executeSql就能触发
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("insert into book_thumbs_per_hour select HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*) from user_behavior where behavior = 'thumbs' GROUP BY TUMBLE(ts, INTERVAL '1' HOUR)");
        }
}
