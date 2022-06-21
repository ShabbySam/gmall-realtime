package com.myPractice.realtime.app.dwd.db;

import com.myPractice.realtime.app.BaseSQLAppV1;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/21 11:17
 *
 * 交易域下单事务事实表
 *  下单 -> 插入
 *  读取订单预处理表数据，筛选insert类型的数据
 */
public class Dwd_06_DwdTradeOrderDetail extends BaseSQLAppV1 {
    public static void main(String[] args) {
        new Dwd_06_DwdTradeOrderDetail().init(
            2006,
            2,
            "Dwd_06_DwdTradeOrderDetail",
            10
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

        tEnv.executeSql("create table dwd_trade_order_pre_process(" +
                " id string, " +
                " order_id string, " +
                " user_id string, " +
                " order_status string, " +
                " sku_id string, " +
                " sku_name string, " +
                " province_id string, " +
                " activity_id string, " +
                " activity_rule_id string, " +
                " coupon_id string, " +
                " date_id string, " +
                " create_time string, " +
                " operate_date_id string, " +
                " operate_time string, " +
                " source_id string, " +
                " source_type string, " +
                " source_type_name string, " +
                " sku_num string, " +
                " split_original_amount string, " +
                " split_activity_amount string, " +
                " split_coupon_amount string, " +
                " split_total_amount string, " +
                " `type` string, " +
                " `old` map<string,string>, " +
                " od_ts string, " +
                " oi_ts string, " +
                " row_op_ts timestamp_ltz(3)" +
                " ) " + SQLUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRADE_ORDER_PRE_PROCESS, "Dwd_06_DwdTradeOrderDetail"));

        Table result = tEnv.sqlQuery("select " +
                "id, " +
                "order_id, " +
                "user_id, " +
                "sku_id, " +
                "sku_name, " +
                "province_id, " +
                "activity_id, " +
                "activity_rule_id, " +
                "coupon_id, " +
                "date_id, " +
                "create_time, " +
                "source_id, " +
                "source_type source_type_code, " +
                "source_type_name, " +
                "sku_num, " +
                "split_original_amount, " +
                "split_activity_amount, " +
                "split_coupon_amount, " +
                "split_total_amount, " +
                "od_ts ts, " +
                "row_op_ts " +
                " from dwd_trade_order_pre_process " +
                " where `type`='insert' ");

        tEnv.executeSql("create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "date_id string, " +
                "create_time string, " +
                "source_id string, " +
                "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ") " + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
        result.executeInsert("dwd_trade_order_detail");
    }
}
