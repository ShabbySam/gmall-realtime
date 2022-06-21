package com.myPractice.realtime.app;

import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.util.FlinkSourceUtil;
import com.myPractice.realtime.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/14 20:21
 */
public abstract class BaseSQLAppV1 {
    /**
     * flinkSQL流创建
     *
     * @param port：web客户端端口
     * @param parallelism：并行度
     * @param ckAndGroupID：类名和kafka消费者组id（同名）
     * @param ttlSecond: 保留时间间隔
     */
    public void init(int port, int parallelism, String ckAndGroupID, long ttlSecond) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);

        env.enableCheckpointing(3000);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/gmall/" + ckAndGroupID);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(ttlSecond));

        handle(env,tEnv);


    }

    protected abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv);

    public void readOdsDb(StreamTableEnvironment tEnv, String groupId){
        tEnv.executeSql("create table ods_db(" +
                " `database` string," +
                " `table` string, " +
                " `type` string, " +
                " `ts` bigint, " +
                " `data` map<string, string>, " +
                " `old` map<string, string>, " +
                " pt as proctime()" +
                ")" + SQLUtil.getKafkaSourceDDL(Constant.TOPIC_ODS_DB, groupId));
    }

    public void readBaseDic(StreamTableEnvironment tEnv){
        tEnv.executeSql("create table base_dic(" +
                " dic_code string, " +
                " dic_name string " +
                ")with(" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop162:3306/gmall2022',\n" +
                "   'table-name' = 'base_dic'," +
                "   'username' = 'root'," +
                "   'lookup.cache.max-rows' = '10',\n" +
                "   'lookup.cache.ttl' = '30 s',\n" +
                "   'password' = 'aaaaaa'" +
                ")");
    }



}
