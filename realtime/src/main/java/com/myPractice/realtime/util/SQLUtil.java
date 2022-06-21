package com.myPractice.realtime.util;

import com.myPractice.realtime.common.Constant;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/20 18:55
 */
public class SQLUtil {
    public static String getKafkaSourceDDL(String topic,String groupId) {
        return "with(" +
                " 'connector'='kafka', " +
                " 'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'topic' = '" + topic + "'," +
                " 'format'='json' " +
                ")";
    }

    public static String getKafkaSinkDDL(String topic) {
        return "with(" +
                " 'connector'='kafka', " +
                " 'properties.bootstrap.servers'='" + Constant.KAFKA_BROKERS + "', " +
                " 'format'='json', " +
                " 'topic'='" + topic + "' " +
                ")";
    }

    public static String getUpsertKafkaSinkDDL(String topic) {
        return "with(" +
                " 'connector'='upsert-kafka', " +
                " 'properties.bootstrap.servers'='" + Constant.KAFKA_BROKERS + "', " +
                " 'key.format'='json', " +
                " 'value.format'='json', " +
                " 'topic'='" + topic + "' " +
                ")";
    }
}
