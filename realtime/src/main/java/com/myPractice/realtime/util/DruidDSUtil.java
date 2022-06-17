package com.myPractice.realtime.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.myPractice.realtime.common.Constant;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/16 16:31
 */
public class DruidDSUtil {
    private static DruidDataSource druidDataSource;

    static {
        // 创建Druid连接池
        druidDataSource = new DruidDataSource();
        // 设置驱动全类名
        druidDataSource.setDriverClassName(Constant.PHOENIX_DRIVER);
        // 设置连接url
        druidDataSource.setUrl(Constant.PHOENIX_URL);
        // 设置初始化连接池时池中连接的数量
        druidDataSource.setInitialSize(5);
        // 设置同时活跃的最大连接数量
        druidDataSource.setMaxActive(100);
        // 设置最小空闲连接数量，在0~maxActive之间，默认为0
        druidDataSource.setMinIdle(1);
        // 设置没有空闲连接时，最大等待时间，超过这个时间，就会抛出异常，默认为-1，表示永不超时一直等待
        druidDataSource.setMaxWait(-1);
        // 验证连接是否可用使用的SQL语句
        druidDataSource.setValidationQuery("SELECT 1");
        // 指明连接是否被空闲连接回收器 （如果有）进行检验，如果检测失败，则连接会被从池中去除
        // 注意默认为true，如果没有设置validationQuery，则报错
        // testWhileIdle is true, validationQuery not set
        druidDataSource.setTestWhileIdle(true);
        // 借出连接时是否检测连接可用性，不测试的话，可以节省性能
        druidDataSource.setTestOnBorrow(false);
        // 归还连接时是否检测连接可用性，不测试的话，可以节省性能
        druidDataSource.setTestOnReturn(false);
        // 设置空闲连接回收器每隔 30s 运行一次
        druidDataSource.setTimeBetweenEvictionRunsMillis(30 * 1000L);
        // 设置池中连接空闲 30min 被回收，默认值即为 30 min
        druidDataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000L);
    }


    public static DruidDataSource getDruidDataSource() {
        return druidDataSource;
    }
}