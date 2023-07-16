package com.vincent.app.dwd.log;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.vincent.utils.DateFormatUtil;
import com.vincent.utils.KafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2、消费kafka topic_log数据流
        String server = "linux1:9092";
        String topic = "topic_log";
        String groupid = "base_log_app";
        DataStreamSource<String> stringDataStreamSource = env.addSource(KafkaUtil.getKafkaConsumer(server, topic, groupid));

        //TODO 3、过滤掉非json数据&将每行数据转换成json对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> process = stringDataStreamSource.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag,value);
                }
            }
        });
        //获取侧输出流并打印
        DataStream<String> sideOutput = process.getSideOutput(dirtyTag);
        sideOutput.print("Dirty>>>>>>>>>>>>>");

        //TODO 4、按照mid分组
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = process.keyBy(value -> value.getJSONObject("common").getString("mid"));

        //TODO 5、使用状态编程做新老访客校验
        SingleOutputStreamOperator<JSONObject> jsonObjectWithNewTag = jsonObjectStringKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                lastVisitState = getRuntimeContext().getState(stringValueStateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //获取is_new & ts
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                //获取状态中日期
                String lastDate = lastVisitState.value();

                //判断is_new标记是否为"1"
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else if (!lastDate.equals(curDate)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastDate == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }

                return value;
            }
        });

        //TODO 6、使用侧输出流分流-分5条流 页面日志放在主流，启动、曝光、动作、错误放在侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        OutputTag<String> actionTag = new OutputTag<String>("action"){};
        OutputTag<String> errorTag = new OutputTag<String>("error"){};
        SingleOutputStreamOperator<String> pageDS = jsonObjectWithNewTag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                //发送错误日志
                String err = value.getString("err");
                if (err != null) {
                    ctx.output(errorTag, value.toJSONString());
                }
                //移除错误信息
                value.remove("err");

                String start = value.getString("start");
                if (start != null) {
                    //发送启动日志
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //获取公共信息&页面id&时间戳
                    String common = value.getString("common");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");

                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject jsonObject = displays.getJSONObject(i);
                            jsonObject.put("common", common);
                            jsonObject.put("page_id", pageId);
                            jsonObject.put("ts", ts);
                            ctx.output(displayTag, jsonObject.toJSONString());
                        }
                    }

                    //尝试获取动作数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject jsonObject = actions.getJSONObject(i);
                            jsonObject.put("common", common);
                            jsonObject.put("page_id", pageId);
                            ctx.output(actionTag, jsonObject.toJSONString());
                        }
                    }

                    //移除曝光&动作数据，写入页面日志主流
                    value.remove("displays");
                    value.remove("actions");
                    out.collect(value.toJSONString());
                }
            }
        });

        //TODO 7、提取各个侧输出流数据
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        //TODO 8、将数据打印并写入对应主题
        pageDS.print("page>>>>>>>>>>>>>");
        startDS.print("start>>>>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>>>>>");
        actionDS.print("action>>>>>>>>>>>>>");
        errorDS.print("error>>>>>>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(KafkaUtil.getKafkaProducer(server,page_topic));
        startDS.addSink(KafkaUtil.getKafkaProducer(server,start_topic));
        displayDS.addSink(KafkaUtil.getKafkaProducer(server,display_topic));
        actionDS.addSink(KafkaUtil.getKafkaProducer(server,action_topic));
        errorDS.addSink(KafkaUtil.getKafkaProducer(server,error_topic));

        //TODO 9、启动任务
        env.execute("BaseLogApp");
    }
}
