package com.lin.kafka;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author lzr
 * @date 2020-12-19 20:00:00
 * 连接kafka数据源写入hive中
 * 获取kafka数据，计算用户注册人数，保存到mysql中
 */
public class ConnectKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.enableCheckpointing(5000);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //kafka配置
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//        properties.setProperty("group.id", "register");

        //kafka数据源
//        DataStreamSource<String> kafkaSource = env
//                .addSource(new FlinkKafkaConsumer<>("register", new SimpleStringSchema(), properties));
//
//        DataStream<AuthUser> dateStream = kafkaSource.map(data -> {
//            AuthUser authUser = JSONObject.parseObject(data, AuthUser.class);
//            return new AuthUser(authUser.getId(), authUser.getCreateTime(), authUser.getName());
//        }).assignTimestampsAndWatermarks(new WatermarkStrategy<AuthUser>() {
//            @Override
//            public WatermarkGenerator<AuthUser> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//                return new WatermarkGenerator<AuthUser>() {
//                    private long maxTimestamp;
//                    private final long delay = 3000;
//
//                    @Override
//                    public void onEvent(AuthUser event, long eventTimestamp, WatermarkOutput output) {
//                        maxTimestamp = Math.max(event.getCreateTime(), maxTimestamp);
//                    }
//
//                    @Override
//                    public void onPeriodicEmit(WatermarkOutput output) {
//                        output.emitWatermark(new Watermark(maxTimestamp - delay));
//                    }
//                };
//            }
//        });

        String name = "hive";
        String database = "hive_test";
        String hiveConf = "/usr/local/Cellar/hive/3.1.2/libexec/conf";

        Catalog catalog = new HiveCatalog(name, database, hiveConf);
        tableEnv.registerCatalog("hive", catalog);
        tableEnv.useCatalog("hive");
        tableEnv.useDatabase("hive_test");

        // 构造kafka流表
        tableEnv.executeSql("DROP TABLE IF EXISTS kafka_auth_user");
        TableResult tableResult2 = tableEnv.executeSql(
                " CREATE TABLE kafka_auth_user (id BIGINT, username STRING, create_time BIGINT, " +
                        "ts AS TO_TIMESTAMP(FROM_UNIXTIME(create_time / 1000, 'yyyy-MM-dd HH:mm:ss'))," +
                        "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND) " +
                        "with (\n" +
                        "    'connector' = 'kafka', \n" +
                        "    'topic' = 'register', \n" +
                        "    'scan.startup.mode' = 'latest-offset', \n" +
                        "    'properties.bootstrap.servers' = '127.0.0.1:9092', \n" +
                        "    'format' = 'json', \n" +
                        "    'json.fail-on-missing-field' = 'false',\n" +
                        "    'json.ignore-parse-errors' = 'true',\n" +
                        "    'is_generic' = 'true')");


        //构建mysql表
        tableEnv.executeSql("DROP TABLE IF EXISTS flink_register_count");
        tableEnv.executeSql("CREATE TABLE flink_register_count (\n" +
                "  start_time TIMESTAMP(3),\n" +
                "  end_time TIMESTAMP(3),\n" +
                "  user_count BIGINT \n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                //UTC为标准时间，即0时区
                "   'url' = 'jdbc:mysql://localhost:3306/cat-book?useUnicode=true&characterEncoding=utf-8&useSSL=true&serverTimezone=UTC',\n" +
                "   'table-name' = 'flink_register_count',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'root'\n" +
                ")");


        // 数据写入，无需提交，使用executeSql就能触发
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql(
                "INSERT INTO flink_register_count " +
                        " SELECT TUMBLE_START(ts, INTERVAL '5' MINUTE) as start_time, TUMBLE_END(ts, INTERVAL '5' MINUTE) as end_time, COUNT(DISTINCT id) as user_count\n" +
                        "FROM kafka_auth_user\n" +
                        "GROUP BY TUMBLE(ts, INTERVAL '5' MINUTE)");



    }
}
