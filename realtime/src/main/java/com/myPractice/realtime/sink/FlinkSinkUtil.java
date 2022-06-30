package com.myPractice.realtime.sink;

import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.annotation.NoSink;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;
import com.myPractice.realtime.bean.TableProcess;
import com.myPractice.realtime.common.Constant;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;


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


    /**
     * Clickhouse的JdbcSink方法
     * @param table：表名
     * @param tClass：用于存数据的bean类的class
     * @param <T>：bean里各个属性的类型
     * @return Clickhouse的jdbcSink
     */
    public static <T> SinkFunction<T> getClickHouseSink(String table, Class<T> tClass) {
        // 使用jdbcSink封装一个clickhouse sink
        String driver = Constant.CLICKHOUSE_DRIVER;
        String url = Constant.CLICKHOUSE_URL;


        // insert into table(age, name, gender) value(?,?,?)
        // 使用反射，找到pojo中的属性名———>复习反射相关
        Field[] fields = tClass.getDeclaredFields();
        // 获取所有的列名(属性名就是列名的驼峰式，顺序要求也是一一对应的)
        String names = Stream
                .of(fields)
                // 过滤掉不需要的字段
                .filter(field -> {
                    NoSink noSink = field.getAnnotation(NoSink.class);
                    // 没有我自定义的noSink注解的才保留下来
                    return noSink == null;
                })
                .map(fild -> {
                    String name = fild.getName();
                    // 驼峰转换成下划线
                    return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
                })
                .collect(Collectors.joining(","));

        String sql = "insert into " + table + "(" + names + ")values(" + names.replaceAll("[^,]+", "?") + ")";
        System.out.println("clickhouse 插入语句:" + sql);
        return getJdbcSink(sql, driver, url, null, null);
    }


    /**
     * 获取jdbcSink
     * @param sql：插入语句
     * @param driver：jdbc驱动全类名
     * @param url：jdbc连接url
     * @param user：用户名
     * @param password：密码
     * @param <T>：bean里各个属性的类型
     * @return jdbcSink
     */
    private static <T> SinkFunction<T> getJdbcSink(String sql, String driver, String url, String user, String password) {
        return JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T t) throws SQLException {
                        // TODO: 要根据sql语句来写,从t对象中，取出值，给sql中的占位符赋值
                        //  insert into a(stt,edt,source,keyword,keyword_count,ts)values(?,?,?,?,?,?)
                        Class<?> tClass = t.getClass();
                        Field[] fields = tClass.getDeclaredFields();
                        try {
                            for (int i = 0, position = 1; i < fields.length; i++) {
                                // 获取第i个属性的对象
                                Field field = fields[i];
                                if (field.getAnnotation(NoSink.class) == null) {
                                    // 暴力开启私有属性访问权限
                                    field.setAccessible(true);
                                    // 通过第i个属性的对象获取第i个属性的值
                                    Object v = field.get(t);
                                    // 给预编译 SQL 语句的对象赋值，占位符下标从1开始，为了跳过不需要的字段采用了单独的计数器position
                                    ps.setObject(position++, v);

                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1024)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(driver)
                        .withUrl(url)
                        .withUsername(user)
                        .withPassword(password)
                        .build()
        );
    }
}
