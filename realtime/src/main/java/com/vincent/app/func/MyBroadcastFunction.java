package com.vincent.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.vincent.bean.TableProcess;
import com.vincent.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class MyBroadcastFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> tableProcessMapStateDescriptor;
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        //super.open(parameters);
        //Class.forName(GmallConfig.PHOENIX_DRIVER);
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    public MyBroadcastFunction(MapStateDescriptor<String, TableProcess> tableProcessMapStateDescriptor) {
        this.tableProcessMapStateDescriptor = tableProcessMapStateDescriptor;
    }

    /*
    maxwell数据格式：
    {
        "database":"gmall",
        "table":"cart_info",
        "type":"update",
        "ts":1592270938,
        "xid":13090,
        "xoffset":1573,
        "data":{
            "id":100924,
            "user_id":"93",
            "sku_id":16,
            "cart_price":4488,
            "sku_num":1,
            "img_url":"http://47.93.148.192:8080/group1/M00/00/02/rBHu8l-sklaALrngAAHGDqdpFtU741.jpg",
            "sku_name":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万超感知徕卡三摄 30倍数字变焦 8GB+128GB亮黑色全网通5G手机",
            "is_checked":null,
            "create_time":"2020-06-14 09:28:57",
            "operate_time":null,
            "is_ordered":1,
            "order_time":"2021-10-17 09:28:58",
            "source_type":"2401",
            "source_id":null
        },
        "old":{
            "is_ordered":0,
            "order_time":null
        }
    }
     */
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //获取广播配置数据
        ReadOnlyBroadcastState<String, TableProcess> tableConfigState = ctx.getBroadcastState(tableProcessMapStateDescriptor);
        //获取配置信息
        String sourceTable = value.getString("table");
        TableProcess tableConfig = tableConfigState.get(sourceTable);
        //筛选维表
        if (tableConfig != null) {
            JSONObject data = value.getJSONObject("data");
            String sinkTable = tableConfig.getSinkTable();

            //根据sinkColumns过滤数据
            String sinkColumns = tableConfig.getSinkColumns();
            filterColumns(data, sinkColumns);

            //将目标表名加入到主流数据中
            data.put("sinkTable", sinkTable);

            out.collect(data);
        }else {
            System.out.println("找不到对应key：" + sourceTable);
        }
    }

    private void filterColumns(JSONObject data, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
//        while(iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if(!columnList.contains(next.getKey())){
//                iterator.remove();
//            }
//        }
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }

    /*
    flinkCDC数据格式：
    {
        "before":null,
        "after":{
            "source_table":"base_trademark",
            "sink_table":"dim_base_trademark",
            "sink_columns":"id,tm_name",
            "sink_pk":"id",
            "sink_extend":null
        },
        "source":{
            "version":"1.5.4.Final",
            "connector":"mysql",
            "name":"mysql_binlog_source",
            "ts_ms":1655172926148,
            "snapshot":"false",
            "db":"gmall-211227-config",
            "sequence":null,
            "table":"table_process",
            "server_id":0,
            "gtid":null,
            "file":"",
            "pos":0,
            "row":0,
            "thread":null,
            "query":null
        },
        "op":"r",
        "ts_ms":1655172926150,
        "transaction":null
    }
     */
    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObject = JSON.parseObject(value);
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(tableProcessMapStateDescriptor);
        String op = jsonObject.getString("op");
        if ("d".equals(op)) {
            TableProcess before = jsonObject.getObject("before", TableProcess.class);
            String sourceTable = before.getSourceTable();
            broadcastState.remove(sourceTable);
        } else {
            //解析数据
            TableProcess config = jsonObject.getObject("after", TableProcess.class);
            String sourceTable = config.getSourceTable();
            String sinkTable = config.getSinkTable();
            String sinkColumns = config.getSinkColumns();
            String sinkPk = config.getSinkPk();
            String sinkExtend = config.getSinkExtend();
            //校验并建表
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
            //写入状态并广播
            broadcastState.put(sourceTable, config);
        }
    }

    /**
     * Phoenix 建表函数
     *
     * @param sinkTable   目标表名  eg. test
     * @param sinkColumns 目标表字段  eg. id,name,sex
     * @param sinkPk      目标表主键  eg. id
     * @param sinkExtend  目标表建表扩展字段
     *                    create table if not exists mydb.test(id varchar primary key, name varchar, sex varchar)...
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        // 封装建表 SQL
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(\n");
        String[] columnArr = sinkColumns.split(",");
        // 为主键及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        // 遍历添加字段信息
        for (int i = 0; i < columnArr.length; i++) {
            sql.append(columnArr[i] + " varchar");
            // 判断当前字段是否为主键
            if (sinkPk.equals(columnArr[i])) {
                sql.append(" primary key");
            }
            // 如果当前字段不是最后一个字段，则追加","
            if (i < columnArr.length - 1) {
                sql.append(",\n");
            }
        }
        sql.append(")");
        sql.append(sinkExtend);
        String createStatement = sql.toString();

        // 为数据库操作对象赋默认值，执行建表 SQL
        PreparedStatement preparedSt = null;
        try {
            preparedSt = conn.prepareStatement(createStatement);
            preparedSt.execute();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
            //System.out.println("建表语句\n" + createStatement + "\n执行异常");
            throw new RuntimeException("建表语句\\n\" + createStatement + \"\\n执行异常");
        } finally {
            if (preparedSt != null) {
                try {
                    preparedSt.close();
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                    throw new RuntimeException("数据库操作对象释放异常");
                }
            }
        }
    }
}
