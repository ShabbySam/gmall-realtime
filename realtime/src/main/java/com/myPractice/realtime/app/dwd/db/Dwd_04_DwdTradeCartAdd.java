package com.myPractice.realtime.app.dwd.db;

import com.myPractice.realtime.app.BaseSQLAppV1;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/20 2:49
 *
 * 交易域加购事务事实表
 *  提取加购操作生成加购表，并将字典表中的相关维度退化到加购表中，写出到 Kafka 对应主题
 *
 *  数据源：
 *      ods_db
 *
 *  过滤：
 *      过滤出来加购物车的数据cart_info
 *
 *      新加入和修改（个数增加导致的）的
 *          例子：
 *          苹果手机 1个
 *          华为手机 1
 *          苹果手机 4    从1变为4
 *              那么新纪录应该是4 or 3
 *                  是3
 *
 *  sql语句：
 *      1. 建立动态表与kafka的topic进行关联：ods_db
 *
 *      2. 过滤出购物车数据
 *
 *      3. 维度退化：base_dic中的维度信息退化到事实表中
 *          join
 *              表与表的join
 *                  事实表和维度表的join
 *                      lookup join
 *
 *      3. 把结果写入kafka
 *
 */
public class Dwd_04_DwdTradeCartAdd extends BaseSQLAppV1 {
    public static void main(String[] args) {
        new Dwd_04_DwdTradeCartAdd().init(2004, 2, "Dwd_04_DwdTradeCartAdd", 5);
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 1. 建立动态表与kafka的topic进行关联：ods_db
        readOdsDb(tEnv,"Dwd_04_DwdTradeCartAdd");
        // 2. lookup join base_dic
        readBaseDic(tEnv);
        // 2. 过滤出加购物车数据
        Table cartInfo = tEnv.sqlQuery("select " +
                "  `data`['id'] id, " +
                "  `data`['user_id'] user_id, " +
                "  `data`['sku_id'] sku_id, " +
                "  if(`type`='insert', " +
                "         `data`['sku_num'], " +
                "         cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)" +
                "  ) sku_num, " +
                "  `data`['source_type'] source_type, " +
                "  `data`['source_id'] source_id, " +
                " `ts`, " +
                " `pt` " +
                "from ods_db " +
                "where `database`='gmall2022' " +
                "and `table`='cart_info' " +
                "and (" +
                "  `type`='insert' " +
                "  or (`type`='update' " +
                "         and `old`['sku_num'] is not null " +
                "         and  cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)  " +
                "     )" +
                "   )");
        tEnv.createTemporaryView("cart_info", cartInfo);

        // 4. lookup join base_dic中的维度信息退化到事实表中
        Table result = tEnv.sqlQuery("select" +
                " ci.id," +
                " ci.user_id," +
                " ci.sku_id," +
                " ci.sku_num," +
                " ci.source_type, " +
                " bd.dic_name source_type_name," +
                " ts " +
                "from cart_info ci " +
                "join base_dic for system_time as of ci.pt as bd " +
                "on ci.source_type=bd.dic_code ");

        // 4. 把结果写入kafka
        tEnv.executeSql("create table dwd_trade_cart_add(" +
                " id string, " +
                " user_id string," +
                " sku_id string, " +
                " sku_num string," +
                " source_type string," +
                " source_type_name string, " +
                " ts bigint" +
                ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_CART_ADD));

        result.executeInsert("dwd_trade_cart_add");
    }
}
