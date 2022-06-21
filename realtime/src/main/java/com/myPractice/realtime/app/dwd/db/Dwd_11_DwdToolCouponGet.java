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
 * @create 2022/6/21 20:54
 *
 * 工具域一般就是使用前 使用中（下单没支付） 使用后
 *
 * 工具域优惠券领取事务事实表
 *  优惠券使用表，领取优惠券后，会生成一条记录----操作类型为 insert
 */
public class Dwd_11_DwdToolCouponGet extends BaseSQLAppV1 {
    public static void main(String[] args) {
        new Dwd_11_DwdToolCouponGet().init(
            2011,
            2,
            "Dwd_11_DwdToolCouponGet",
            10
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取ods_db数据
        readOdsDb(tEnv, "Dwd_11_DwdToolCouponGet");
        // 2. 过滤优惠券领用:  insert 数据
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
                " and `type`='insert'");

        // 3. 写出到kafka中
        tEnv.executeSql("create table dwd_tool_coupon_get (  " +
                "id string,  " +
                "coupon_id string,  " +
                "user_id string ,  " +
                "date_id string,  " +
                "get_time string,  " +
                "ts string  " +
                ")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_TOOL_COUPON_GET));

        couponUse.executeInsert("dwd_tool_coupon_get");
    }
}
