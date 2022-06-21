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
 * @create 2022/6/21 21:13
 *
 * 工具域优惠券使用(支付)事务事实表
 *  优惠券使用表，下单后，会修改优惠券状态-------操作类型为 update，
 *                                           修改了 used_time 字段
 *                                           coupon_status 字段的值为 1403（已使用）
 */
public class Dwd_13_DwdToolCouponPay extends BaseSQLAppV1 {
    public static void main(String[] args) {
        new Dwd_13_DwdToolCouponPay().init(
                2011,
                2,
                "Dwd_13_DwdToolCouponPay",
                10
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取ods_db数据
        readOdsDb(tEnv, "Dwd_13_DwdToolCouponPay");
        // 2. 过滤优惠券使用:  update 数据
        Table couponUse = tEnv.sqlQuery("select " +
                "data['id'] id,  " +
                "data['coupon_id'] coupon_id,  " +
                "data['user_id'] user_id,  " +
                "date_format(data['get_time'],'yyyy-MM-dd') date_id,  " +
                "data['get_time'] get_time,  " +
                "cast(ts as string) ts " +
                " from ods_db " +
                " where `database`='gmall2022' " +
                " and `table`='coupon_use' " +
                " and `type`='update' " +
                " and `old`['coupon_status'] is not null " +
                " and `data`['coupon_status']='1403'");
        // 3. 写出到kafka
        tEnv.executeSql("create table dwd_tool_coupon_pay( " +
                "id string,  " +
                "coupon_id string,  " +
                "user_id string,  " +
                "date_id string,  " +
                "used_time string,  " +
                "ts string  " +
                " ) " + SQLUtil.getKafkaSinkDDL(Constant.KAFKA_TOPIC_DWD_TOOL_COUPON_PAY));

        couponUse.executeInsert("dwd_tool_coupon_pay");
    }
}
