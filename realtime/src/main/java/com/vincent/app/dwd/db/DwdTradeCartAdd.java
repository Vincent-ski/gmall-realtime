package com.vincent.app.dwd.db;

import com.vincent.utils.KafkaUtil;
import com.vincent.utils.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设定 Table 中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        // 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // 为表关联时状态中存储的数据设置过期时间
        configuration.setString("table.exec.state.ttl", "5 s");

        // TODO 2. 状态后端设置
        /*
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://linux1:8020/ck");
        System.setProperty("HADOOP_USER_NAME", "vincent");
        */

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("" +
                "create table topic_db( " +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>, " +
                "`ts` string, " +
                "`proc_time` as PROCTIME() " +
                ")" + KafkaUtil.getKafkaDDL("linux1:9092","topic_db", "dwd_trade_cart_add"));

        // TODO 4. 读取购物车表数据
        Table cartAdd = tableEnv.sqlQuery("" +
                "select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['sku_id'] sku_id, " +
                "data['source_id'] source_id, " +
                "data['source_type'] source_type, " +
                "if(`type` = 'insert', " +
                "data['sku_num'],cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num, " +
                "ts, " +
                "proc_time " +
                "from `topic_db`  " +
                "where `table` = 'cart_info' " +
                "and (`type` = 'insert' " +
                "or (`type` = 'update'  " +
                "and `old`['sku_num'] is not null  " +
                "and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        tableEnv.createTemporaryView("cart_add", cartAdd);

        // TODO 5. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 6. 关联两张表获得加购明细表
        Table resultTable = tableEnv.sqlQuery("select " +
                "cadd.id, " +
                "user_id, " +
                "sku_id, " +
                "source_id, " +
                "source_type source_type_id, " +
                "dic_name source_type_name, " +
                "sku_num, " +
                "ts " +
                "from cart_add cadd " +
                "join base_dic for system_time as of cadd.proc_time as dic " +
                "on cadd.source_type=dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 7. 建立 Kafka-Connector dwd_trade_cart_add 表
        tableEnv.executeSql("" +
                "create table dwd_trade_cart_add( " +
                "id string, " +
                "user_id string, " +
                "sku_id string, " +
                "source_id string, " +
                "source_type_id string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "ts string " +
                ")" + KafkaUtil.getKafkaSinkDDL("linux1:9092","dwd_trade_cart_add"));

        // TODO 8. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_cart_add select * from result_table");
    }
}

