package com.myPractice.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.bean.TableProcess;
import com.myPractice.realtime.util.DruidDSUtil;
import com.myPractice.realtime.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/16 16:27
 */
public class PhoenixSink extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {

    private DruidPooledConnection connection;
    private Jedis redisClient;

    /**
     * 获取phoenix连接,获取redis连接
     * @param parameters 参数
     * @throws Exception 异常
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 使用phoenix连接池——Druid连接池
        DruidDataSource druidDataSource = DruidDSUtil.getDruidDataSource();
        connection = druidDataSource.getConnection();
        // 使用redis连接池
        redisClient = RedisUtil.getRedisClient();
    }

    /**
     * 关闭phoenix连接（使用连接池的时候是归还）,关闭redis连接
     * @throws Exception 异常
     */
    @Override
    public void close() throws Exception {
        if (connection != null) {
            // 如果是从连接池获取的连接，close是归还连接池
            connection.close();
        }
    }


    /**
     * 执行phoenix插入操作，更新redis中的缓存
     * @param value 元组（json对象，表处理对象）
     * @param context 上下文
     * @throws Exception 异常
     */
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        // 1. 写入phoenix
        writeToPhoenix(value);

        // 2. 更新缓存或者删除缓存
        delCache(value);

    }

    /**
     *
     * @param value
     */
    private void delCache(Tuple2<JSONObject, TableProcess> value) {
        JSONObject data = value.f0;
        TableProcess tp = value.f1;

        if ("update".equals(tp.getOperate_type())) {
            // key：表名:id
            System.out.println("开始删除");
            String key = tp.getSinkTable() + ":" + data.getString("id");
            redisClient.del(key); // 删除的时候key不存在怎么办：不会报错，不管
        }

    }

    private void writeToPhoenix(Tuple2<JSONObject, TableProcess> value) throws SQLException {

        JSONObject data = value.f0;
        TableProcess tableProcess = value.f1;

        // 1. 拼接sql  upsert into t(a,b,c) values(?,?,?)
        StringBuilder sql = new StringBuilder();
        sql.append("upsert into ")
                .append(tableProcess.getSinkTable())
                .append("(")
                .append(tableProcess.getSinkColumns())
                .append(")")
                .append("values(")
                .append(tableProcess.getSinkColumns().replaceAll("[^,]+", "?"))
                .append(")");
        System.out.println("插入语句: " + sql.toString());


        // 2. 根据sql得到预处理语句
        PreparedStatement ps = connection.prepareStatement(sql.toString());

        // 3. 给占位符赋值 todo
        String[] columnNames = tableProcess.getSinkColumns().split(",");
        for (int i = 0; i < columnNames.length; i++) {
            // 注意 columnName有可能是空的，所以要判断，v == null ? "" : v ， 不能直接v + ""，因为null + "" = null，会报错
            ps.setString(i + 1, data.get(columnNames[i]) == null ? null : data.get(columnNames[i]).toString());
        }

        // 4. 执行预处理语句
        ps.execute();

        // 5. 提交 dml操作需要提交, 如果是dql，ddl操作，不需要提交，mysql会自动提交
        connection.commit();

        // 6. 关闭连接
        ps.close();
    }
}
