package com.myPractice.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/15 18:25
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableProcess {
    /**
     * 配置表
     *   原本需要跟列名严格一致，但fastjson可以下划线/驼峰自动转换
     */
    private String sourceTable;
    private String sinkTable;
    private String sinkColumns;
    private String sinkPk;
    private String sinkExtend;

    // 用于给redis判断是不是更新
    private String operate_type;
}
