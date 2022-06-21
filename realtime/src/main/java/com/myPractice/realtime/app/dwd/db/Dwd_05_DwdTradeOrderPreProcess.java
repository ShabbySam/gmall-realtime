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
 * @create 2022/6/20 21:00
 *
 * 订单明细表和取消订单明细表
 *  数据来源、表结构都相同
 *  差别只在业务过程和过滤条件
 *      下单：
 *          order_info新增一条记录 order_detail新增多条记录
 *      取消订单：
 *          order_info更新一条记录 order_detail没有变化
 *
 * -----------------------------------------------------------------------
 *
 * 预处理表：
 *  将两张表公共的关联过程提取出来，形成订单预处理表
 *
 *  order_info order_detail 详情活动 详情优惠券 字典表
 *      order_info join order_detail
 *      详情活动 详情优惠券：left join
 *      字典表：lookup join
 */
public class Dwd_05_DwdTradeOrderPreProcess extends BaseSQLAppV1 {
    public static void main(String[] args) {
        new Dwd_05_DwdTradeOrderPreProcess().init(
            2005,
            2,
            "Dwd_05_DwdTradeOrderPreProcess",
            10
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 消费ods_db数据
        readOdsDb(tEnv,"Dwd_05_DwdTradeOrderPreProcess");

        // 2. 读取字典表
        readBaseDic(tEnv);

        // 3. 从ods_db过滤出订单详情表
        Table orderDetail = tEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['order_id'] order_id, " +
                " `data`['sku_id'] sku_id, " +
                " `data`['sku_name'] sku_name, " +
                " `data`['sku_num'] sku_num, " +
                " `data`['create_time'] create_time, " +
                " `data`['source_type'] source_type, " +
                " `data`['source_id'] source_id, " +
                " `data`['split_total_amount'] split_total_amount, " +
                " `data`['split_activity_amount'] split_activity_amount, " +
                " `data`['split_coupon_amount'] split_coupon_amount, " +
                " cast(cast(`data`['order_price'] as decimal(16,2)) * cast(`data`['sku_num'] as decimal(16,2)) as string) split_original_amount, " +
                " pt, " +
                " ts od_ts " +
                " from ods_db " +
                " where `database` = 'gmall2022' " +
                " and `table` = 'order_detail' " +
                " and `type`='insert' ");
        tEnv.createTemporaryView("order_detail", orderDetail);

        // 4. 过滤出订单表
        Table orderInfo = tEnv.sqlQuery("select " +
                " `data`['id'] id, " +
                " `data`['user_id'] user_id, " +
                " `data`['province_id'] province_id, " +
                " `data`['order_status'] order_status, " +
                " `data`['create_time'] create_time, " +
                " `data`['operate_time'] operate_time, " +
                " `type`, " +
                " `old`, " +
                " ts oi_ts " +
                " from ods_db " +
                " where `database` = 'gmall2022' " +
                " and `table` = 'order_info' " +
                " and (`type`='insert' " +
                " or `type`='update') ");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 5. 过滤出活动表
        Table orderDetailActivity = tEnv.sqlQuery("select " +
                " `data`['order_detail_id'] order_detail_id, " +
                " `data`['activity_id'] activity_id, " +
                " `data`['activity_rule_id'] activity_rule_id " +
                " from ods_db " +
                " where `database` = 'gmall2022' " +
                " and `table` = 'order_detail_activity' " +
                " and `type`='insert' ");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 6. 过滤出优惠券表
        Table orderDetailCoupon = tEnv.sqlQuery("select " +
                " `data`['order_detail_id'] order_detail_id, " +
                " `data`['coupon_id'] coupon_id, " +
                " `data`['coupon_use_id'] coupon_use_id " +
                "from ods_db " +
                "where `database`='gmall2022' " +
                "and `table`='order_detail_coupon' " +
                "and `type`='insert'");
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);

        // 7. join
        Table result = tEnv.sqlQuery("select " +
                " od.id, " +
                " od.order_id, " +
                " oi.user_id, " +
                " oi.order_status, " +
                " od.sku_id, " +
                " od.sku_name, " +
                " oi.province_id, " +
                " act.activity_id, " +
                " act.activity_rule_id, " +
                " cou.coupon_id, " +
                " date_format(od.create_time, 'yyyy-MM-dd') date_id, " +  // 年月日
                " od.create_time, " +
                " date_format(oi.operate_time, 'yyyy-MM-dd') operate_date_id, " +
                " oi.operate_time, " +
                " od.source_id, " +
                " od.source_type, " +
                " dic.dic_name source_type_name, " +
                " od.sku_num, " +
                " od.split_original_amount, " +
                " od.split_activity_amount, " +
                " od.split_coupon_amount, " +
                " od.split_total_amount, " +
                " oi.`type`, " +
                " oi.`old`, " +
                " cast (od.od_ts as string) od_ts, " +
                " cast (oi.oi_ts as string) oi_ts, " +
                " current_row_timestamp() row_op_ts " +  // dws用于过滤重复数据，壹left join为例子，当左表先来右表没来的时候右表给null，之后右表来了就会新写一条数据，而通挑选日期最新的时间挑选最完整的数据
                " from order_detail od " +
                " join order_info oi on od.order_id=oi.id" +
                " join base_dic for system_time as of od.pt as dic on od.source_type=dic.dic_code " +
                " left join order_detail_activity act on od.id=act.order_detail_id " +
                " left join order_detail_coupon cou on od.id=cou.order_detail_id");


        // 8. 预处理表写入kafka
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
                " row_op_ts timestamp_ltz(3)," +
                " primary key(id) not enforced " +
                " ) " + SQLUtil.getUpsertKafkaSinkDDL(Constant.TOPIC_DWD_TRADE_ORDER_PRE_PROCESS));
        //result.executeInsert("dwd_trade_order_pre_process"); // 字段顺序要求一样
        tEnv.executeSql("insert into dwd_trade_order_pre_process select * from " + result); // 字段顺序要求一样
    }
}
