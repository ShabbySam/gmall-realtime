package com.myPractice.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.common.Constant;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/29 0:13
 */
public class DimUtil {
    public static JSONObject readDimFromPhoenix(Connection phoenixConn, String dimTableName, String id, Boolean... underlineToCaseCamel ) {
        // select * from t where id=?
        String sql = "select * from " + dimTableName + " where id=?";
        Object[] args = {id};
        List<JSONObject> result = null;
        try {
            result = JdbcUtil.<JSONObject>queryList(phoenixConn, sql, args, JSONObject.class, underlineToCaseCamel);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("sql语句有问题, 请检查sql的拼接是否正常..." + sql);
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new RuntimeException("请给 JSONObject 提供无参构造器");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException("请检查你的无参构造器是否有public权限...");
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException("在 JSONObject 对象找不到对应的属性....");
        }

        if (result.size() == 0) {
            throw new RuntimeException("没有查到对应的维度数据, 请检查表是否存在, 维度数据是否存在: 表名->" + dimTableName + " id->" + id);
        }
        return result.get(0);
    }


    /**
     * 读取维度数据
     * @param redisClient redis客户端
     * @param phoenixConn   phoenix连接
     * @param dimTableName 维度表名
     * @param id 维度id
     * @return 维度数据
     */
    public static JSONObject readDim(Jedis redisClient, Connection phoenixConn, String dimTableName, String id) {
        // 1. 从redis读取维度数据
        JSONObject dim = readFromRedis(redisClient, dimTableName, id);

        // 2. 如果存在则吧读到的维度数据返回
        if (dim == null) {
            // 3. 如果不存在则从phoenix读取维度数据, 把读取到的数据存入redis中，然后再把数据返回
            dim = readDimFromPhoenix(phoenixConn, dimTableName, id);
            // 把读取到的数据存入redis中
            writeToRedis(redisClient, dimTableName, id, dim);
            System.out.println(dimTableName + " " + id + " 查询的数据库....");

        } else {
            System.out.println(dimTableName + " " + id + " 查询的redis....");
        }

        return dim;
    }


    /**
     * 往redis中写入维度数据
     * @param redisClient redis连接
     * @param dimTableName 维度表名
     * @param id 维度id
     * @param dim 维度数据
     */
    private static void writeToRedis(Jedis redisClient, String dimTableName, String id, JSONObject dim) {
        String key = dimTableName + ":" + id;
/*
        redisClient.set(key, dim.toJSONString());
        // 给key设置ttl
        redisClient.expire(key, Constant.DIM_TTL);
        */

        redisClient.setex(key, Constant.DIM_TTL, dim.toJSONString());
    }


    /**
     * 从redis中读取维度数据
     * @param redisClient redis连接
     * @param dimTableName 维度表名
     * @param id 维度id
     * @return 维度数据
     */
    private static JSONObject readFromRedis(Jedis redisClient, String dimTableName, String id) {
        String key = dimTableName + ":" + id;

        String dimJson = redisClient.get(key);

        JSONObject dim = null;

        if (dimJson != null) {
            // 查到维度数据
            dim = JSONObject.parseObject(dimJson);
        }

        return dim;
    }
}
