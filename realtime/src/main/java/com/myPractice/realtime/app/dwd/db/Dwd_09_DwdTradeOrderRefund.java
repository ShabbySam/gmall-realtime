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
 * @create 2022/6/21 18:44
 *
 * 交易域退单事务事实表
 *  退单表  订单表 字典表
 *      订单信息获取省份之类的信息 join
 *      字典表回去退款类型/原因名称 lookup join
 */
public class Dwd_09_DwdTradeOrderRefund extends BaseSQLAppV1 {
    public static void main(String[] args) {
        //退单表  order_info
        new Dwd_09_DwdTradeOrderRefund().init(
            2009,
            2,
            "Dwd_09_DwdTradeOrderRefund",
            10  // 发起退单, 新增一条退单数据, order_info会同时产生一条更新数据, ttl值仅需考虑网络延迟问题
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取ods_db
        readOdsDb(tEnv, "Dwd_09_DwdTradeOrderRefund");
        // 2. 读取 base_dic
        readBaseDic(tEnv);
        // 3. 过滤出来order_info
        Table orderInfo = tEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['province_id'] province_id, " +
                " `old` " +
                " from ods_db " +
                " where `database`='gmall2022' " +
                " and `table`='order_info' " +
                " and `old`['order_status'] is not null " +
                " and `data`['order_status']='1005'");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 4. 过滤出退单表
        Table orderRefundInfo = tEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['sku_id'] sku_id, " +
                "data['refund_type'] refund_type, " +
                "data['refund_num'] refund_num, " +
                "data['refund_amount'] refund_amount, " +
                "data['refund_reason_type'] refund_reason_type, " +
                "data['refund_reason_txt'] refund_reason_txt, " +
                "data['create_time'] create_time, " +
                "pt, " +
                "ts " +
                " from ods_db " +
                " where `database`='gmall2022' " +
                " and `table`='order_refund_info' " +
                " and `type`='insert'");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 5. join 三张表
        Table result = tEnv.sqlQuery("select " +
                "ri.id,  " +
                "ri.user_id,  " +
                "ri.order_id,  " +
                "ri.sku_id,  " +
                "oi.province_id,  " +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id,  " +
                "ri.create_time,  " +
                "ri.refund_type,  " +
                "dic1.dic_name,  " +
                "ri.refund_reason_type,  " +
                "dic2.dic_name,  " +
                "ri.refund_reason_txt,  " +
                "ri.refund_num,  " +
                "ri.refund_amount,  " +
                "cast(ri.ts as string),  " +
                "current_row_timestamp() row_op_ts " +
                " from order_refund_info ri " +
                " join order_info oi on ri.order_id=oi.id " +
                " join base_dic for system_time as of ri.pt as dic1 on ri.refund_type=dic1.dic_code " +
                " join base_dic for system_time as of ri.pt as dic2 on ri.refund_reason_type=dic2.dic_code");

        // 6. 写出到kafka
        tEnv.executeSql("create table dwd_trade_order_refund( " +
                "id string,  " +
                "user_id string,  " +
                "order_id string,  " +
                "sku_id string,  " +
                "province_id string,  " +
                "date_id string,  " +
                "create_time string,  " +
                "refund_type_code string,  " +
                "refund_type_name string,  " +
                "refund_reason_type_code string,  " +
                "refund_reason_type_name string,  " +
                "refund_reason_txt string,  " +
                "refund_num string,  " +
                "refund_amount string,  " +
                "ts string,  " +
                "row_op_ts timestamp_ltz(3)  " +
                " )"+ SQLUtil.getKafkaSinkDDL(Constant.DWD_TRADE_ORDER_REFUND));
        result.executeInsert("dwd_trade_order_refund");


    }
}
