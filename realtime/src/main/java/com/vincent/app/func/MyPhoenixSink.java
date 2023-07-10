package com.vincent.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.vincent.utils.DruidUtil;
import com.vincent.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

public class MyPhoenixSink extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 创建连接池
        druidDataSource = DruidUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        // 获取目标表表名
        String sinkTable = jsonObj.getString("sinkTable");
        // 获取 id 字段的值
        String id = jsonObj.getString("id");

        // 清除 JSON 对象中的 sinkTable 字段
        // 以便可将该对象直接用于 HBase 表的数据写入
        jsonObj.remove("sinkTable");

        // 获取连接对象
        DruidPooledConnection conn = druidDataSource.getConnection();

        try {
            PhoenixUtil.insertValues(conn, sinkTable, jsonObj);
        } catch (Exception e) {
            System.out.println("维度数据写入异常");
            e.printStackTrace();
        } finally {
            try {
                // 归还数据库连接对象
                conn.close();
            } catch (SQLException sqlException) {
                System.out.println("数据库连接对象归还异常");
                sqlException.printStackTrace();
            }
        }
    }

}
