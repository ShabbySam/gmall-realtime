package com.myPractice.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.bean.TableProcess;
import com.myPractice.realtime.common.Constant;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/16 16:19
 */
public class FlinkSinkUtil {
    /**
     * PhoenixSink方法
     * @return PhoenixSinkFunction
     */
    public static SinkFunction<Tuple2<JSONObject, TableProcess>> getPhoenixSink() {
        return new PhoenixSink();

    }

    /**
     * KafkaSink方法
     * @param topic：kafka topic
     * @return KafkaSinkFunction
     */
    public static SinkFunction<String> getKafkaSink(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", Constant.KAFKA_BROKERS);
        props.put("transaction.timeout.ms", 15 * 60 * 1000);

        return new FlinkKafkaProducer<String>(
            "default",
            new KafkaSerializationSchema<String>() {
                @Override
                public ProducerRecord<byte[], byte[]> serialize(String element,
                                                                @Nullable Long timestamp) {
                    return new ProducerRecord<>(topic,element.getBytes(StandardCharsets.UTF_8));
                }
            },
            props,
            FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
