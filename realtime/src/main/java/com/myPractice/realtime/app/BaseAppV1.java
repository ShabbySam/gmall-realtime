package com.myPractice.realtime.app;

import com.myPractice.realtime.util.FlinkSourceUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/14 20:21
 */
public abstract class BaseAppV1 {
    /**
     * flink流创建
     *
     * @param port：web客户端端口
     * @param parallelism：并行度
     * @param ckAndGroupID：类名和kafka消费者组id（同名）
     * @param topic：kafka主题
     */
    public void init(int port, int parallelism, String ckAndGroupID, String topic) {
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

        DataStreamSource<String> stream = env.addSource(FlinkSourceUtil.getKafkaSource(ckAndGroupID, topic));

        handle(env, stream);

        try {
            env.execute(ckAndGroupID);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 处流理的抽象方法
     * @param env: 执行流程序的环境
     * @param stream：创建的流
     */
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);
}
