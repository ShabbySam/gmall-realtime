package com.myPractice.realtime.util;

import com.myPractice.realtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/14 19:58
 */
public class FlinkSourceUtil {
    public static SourceFunction<String> getKafkaSource(String groupID, String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.put("group.id", groupID);
        props.put("isolation.level", "read_committed");

        return new FlinkKafkaConsumer<String>(topic,
                // 自定义反序列化器
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    // 解决消费kafka数据时遇到的null值，有null是因为kafka的数据更新过程写入的
                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        if (record.value() != null) {
                            return null;
                        }
                        return new String(record.value(), StandardCharsets.UTF_8);
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                },
                props
        );
    }
}
