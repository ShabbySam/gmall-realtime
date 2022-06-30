package com.myPractice.realtime.function;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/29 21:47
 */
public interface DimFunction<T> {
    /**
     * 读取维度ID
     * @param input 输入数据
     * @return 维度ID
     */
    String getId(T input);

    /**
     * 获取表名
     * @return 表名
     */
    String getTable();

    /**
     * 把读到的维度数据存入到 input对象中
     * @param input 输入数据
     * @param dim 维度数据
     */
    void addDim(T input, JSONObject dim);
}
