package com.vincent.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.vincent.app.func.MyBroadcastFunction;
import com.vincent.app.func.MyPhoenixSink;
import com.vincent.bean.TableProcess;
import com.vincent.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


//数据流：web/app -> nginx -> 业务服务器 -> mysql(binlog) -> maxwell -> kafka -> flink -> phoenix
//程序：mock -> mysql(binlog) -> maxwell -> kafka(zk) -> DimApp -> phoenix(zk/hdfs/hbase)
public class DimApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
        //1.1 开启检查点
        env.enableCheckpointing(5*60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10*60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,5000L));

        //1.2 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://linux1:8020/checkpoint/");
        System.setProperty("HADOOP_USER_NAME","vincent");
        */

        //TODO 2.读取topic_db创建主流
        String server = "linux1:9092";
        String topic = "topic_db";
        String groupid = "dim_app";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getFlinkKafkaConsumer(server, topic, groupid));

        //TODO 3.过滤掉非json数据 & 保留增删改以及初始化数据
        SingleOutputStreamOperator<JSONObject> jsonObjectSingleOutputStreamOperator = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("脏数据：" + value);
                }
            }
        });

        //TODO 4.使用flinkCDC读取MySql配置信息创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("linux1")
                .port(3306)
                .username("root")
                .password("211819")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        //TODO 5、将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> stringTableProcessMapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlSourceDS.broadcast(stringTableProcessMapStateDescriptor);

        //TODO 6、连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectDS = jsonObjectSingleOutputStreamOperator.connect(broadcastDS);

        //TODO 7、处理连接流，根据配置信息处理主流数据
        SingleOutputStreamOperator<JSONObject> process = connectDS.process(new MyBroadcastFunction(stringTableProcessMapStateDescriptor));

        //TODO 8、将数据写入Phoenix
        DataStreamSink<JSONObject> jsonObjectDataStreamSink = process.addSink(new MyPhoenixSink());

        //TODO 9、启动任务
        env.execute();
    }
}
