package com.myPractice.realtime.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/29 18:06
 *
 * redis连接池
 */
public class RedisUtil {
    private static JedisPool pool;

    static {
        JedisPoolConfig poolConfig = new  JedisPoolConfig();
        // 最大连接对象
        poolConfig.setMaxTotal(100);
        // 允许最大空闲连接对象
        poolConfig.setMaxIdle(10);
        // 最小空闲连接对象
        poolConfig.setMinIdle(2);
        // 从连接池获取/创建/归还连接的时候要不要测试
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnCreate(true);
        poolConfig.setTestOnReturn(true);
        // 最大等待连接时间
        poolConfig.setMaxWaitMillis(10 * 1000);

        pool = new JedisPool(poolConfig, "hadoop162");
    }

    public static Jedis getRedisClient() {
        Jedis client = pool.getResource();
        client.select(1);// 设置分区为1，避免key混乱
        return client;
    }

}
