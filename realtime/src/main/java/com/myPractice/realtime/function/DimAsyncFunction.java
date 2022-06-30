package com.myPractice.realtime.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.Executor;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/29 20:48
 *
 * 异步方法
 *  通过线程池和jdbc连接池，自定义异步方法读取dim，实现异步操作
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimFunction<T> {

    private Executor threadPool;
    private DruidDataSource druidDataSource;


    /**
     * 获取线程池对象和phoenix德鲁伊池对象
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPool();

        druidDataSource = DruidDSUtil.getDruidDataSource();
    }


    /**
     * 异步读取方法实现
     * @param input 输入数据
     * @param resultFuture 异步结果
     * @throws Exception 异常
     */
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        // 多线程 + 多客户端 -> 搞个线程池
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                // 读取维度数据
                Jedis redisClient = RedisUtil.getRedisClient();
                Connection phoenixConnection = null;
                try {
                    phoenixConnection = druidDataSource.getConnection();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                // 使用客户端去读取维度
                JSONObject dim = DimUtil.readDim(redisClient, phoenixConnection, getTable(), getId(input));

                // 把读到的维度数据再存入到 input对象中
                addDim(input, dim);

                resultFuture.complete(Collections.singletonList(input));

                // 关闭
                if (redisClient != null) {
                    redisClient.close();
                }
                if (phoenixConnection != null) {
                    try {
                        phoenixConnection.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }




        });
    }
}
