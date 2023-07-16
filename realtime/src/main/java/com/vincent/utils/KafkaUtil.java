package com.vincent.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

public class KafkaUtil {

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String brokerList, String topic, String groupId){

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        return new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        if (consumerRecord == null || consumerRecord.value() == null){
                            return null;
                        }else{
                            return new String(consumerRecord.value());
                        }
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },
                properties
        );
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String brokerList, String topic) {

        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60 * 15 * 1000 + "");

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                topic,
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long timestamp) {
                        if (jsonStr == null){
                            return null;
                        }
                        return new ProducerRecord<byte[], byte[]>(topic, jsonStr.getBytes());
                    }
                },
                prop,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        return producer;
    }

}
