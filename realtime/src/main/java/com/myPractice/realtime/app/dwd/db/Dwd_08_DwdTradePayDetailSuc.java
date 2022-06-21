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
 * @create 2022/6/21 16:01
 *
 * 交易域支付成功事务事实表
 *  支付相关：payment_info--->订单级别的支付信息
 *  订单详情：交易域下单事务事实表---->join后获得每个商品的支付信息
 *  字典表
 *  payment_info
 *      dwd_trade_order_detail : join
 *      lookup join base_dic : lookup join
 */
public class Dwd_08_DwdTradePayDetailSuc extends BaseSQLAppV1 {
    public static void main(String[] args) {
        new Dwd_08_DwdTradePayDetailSuc().init(
            2008,
            2,
            "Dwd_08_DwdTradePayDetailSuc",
            30 * 60  // 30分钟的ttl: 详情要远远的早于支付
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取ods_db
        readOdsDb(tEnv, "Dwd_08_DwdTradePayDetailSuc");

        // 2. 读取字典表
        readBaseDic(tEnv);

        // 3. 读取详情表dwd_trade_order_detail
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
                ") " + SQLUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL,"Dwd_08_DwdTradePayDetailSuc"));

        // 4. 从ods_db中过滤出payment_info表中支付成功数据
        Table paymentInfo = tEnv.sqlQuery("select " +
                " `data`['user_id'] user_id, " +
                " `data`['order_id'] order_id, " +
                " `data`['payment_type'] payment_type, " +
                " `data`['callback_time'] callback_time, " +
                " pt, " +
                " ts " +
                " from ods_db " +
                " where `database`='gmall2022' " +
                " and `table`='payment_info' " +
                " and `type`='update' " +
                " and `old`['payment_status'] is not null " +
                " and `data`['payment_status']='1602' ");
        tEnv.createTemporaryView("payment_info", paymentInfo);

        // 5. 三张表join
        Table result = tEnv.sqlQuery("select " +
                "od.id order_detail_id, " +
                "od.order_id, " +
                "od.user_id, " +
                "od.sku_id, " +
                "od.sku_name, " +
                "od.province_id, " +
                "od.activity_id, " +
                "od.activity_rule_id, " +
                "od.coupon_id, " +
                "pi.payment_type payment_type_code, " +
                "dic.dic_name payment_type_name, " +
                "pi.callback_time, " +
                "od.source_id, " +
                "od.source_type_code, " +
                "od.source_type_name, " +
                "od.sku_num, " +
                "od.split_original_amount, " +
                "od.split_activity_amount, " +
                "od.split_coupon_amount, " +
                "od.split_total_amount split_payment_amount, " +
                "cast(pi.ts as string) ts, " +
                "od.row_op_ts row_op_ts " +
                " from payment_info pi " +
                " join dwd_trade_order_detail od on pi.order_id = od.order_id " +
                " join base_dic for system_time as of pi.pt as dic on pi.payment_type=dic.dic_code ");


        // 6. 写出kafka
        tEnv.executeSql("create table dwd_trade_pay_detail_suc(" +
                "order_detail_id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                "payment_type_code string, " +
                "payment_type_name string, " +
                "callback_time string, " +
                "source_id string, " +
                "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_payment_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC));
        result.executeInsert("dwd_trade_pay_detail_suc");
    }
}
