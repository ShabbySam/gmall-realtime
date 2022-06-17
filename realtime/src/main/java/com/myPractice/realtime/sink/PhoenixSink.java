package com.myPractice.realtime.sink;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.bean.TableProcess;
import com.myPractice.realtime.util.DruidDSUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

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

    /**
     * 获取phoenix连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        // 使用phoenix连接池——Druid连接池
        DruidDataSource druidDataSource = DruidDSUtil.getDruidDataSource();
        connection = druidDataSource.getConnection();
    }

    /**
     * 关闭phoenix连接（使用连接池的时候是归还）
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if (connection != null) {
            // 如果是从连接池获取的连接，close是归还连接池
            connection.close();
        }
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        // 写入phoenix
        writeToPhoenix(value);

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
