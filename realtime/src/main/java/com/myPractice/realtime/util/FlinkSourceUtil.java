package com.myPractice.realtime.util;

import com.myPractice.realtime.common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

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

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }
}
