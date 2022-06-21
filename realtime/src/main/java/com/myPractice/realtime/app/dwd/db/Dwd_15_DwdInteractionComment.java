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
 * @create 2022/6/21 21:28
 *
 * 互动域评价事务事实表
 * 评价一次，新增一条数据-------操作类型为 insert，
 *  评论表 lookup join 字典表
 */
public class Dwd_15_DwdInteractionComment extends BaseSQLAppV1 {
    public static void main(String[] args) {
        new Dwd_15_DwdInteractionComment().init(
                2011,
                2,
                "Dwd_15_DwdInteractionComment",
                10
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取ods_db数据
        readOdsDb(tEnv, "Dwd_15_DwdInteractionComment");
        // 2. 读取字典表
        readBaseDic(tEnv);
        // 3. 过滤出来评价表
        Table commentInfo = tEnv.sqlQuery("select " +
                "data['id'] id,  " +
                "data['user_id'] user_id,  " +
                "data['sku_id'] sku_id,  " +
                "data['order_id'] order_id,  " +
                "data['create_time'] create_time,  " +
                "data['appraise'] appraise, " +
                "pt,  " +
                "ts  " +
                " from ods_db" +
                " where `database`='gmall2022' " +
                " and `table`='comment_info' " +
                " and `type`='insert'");
        tEnv.createTemporaryView("comment_info", commentInfo);

        // 4. 评价表和维度表做join
        Table result = tEnv.sqlQuery("select " +
                "ci.id,  " +
                "ci.user_id,  " +
                "ci.sku_id,  " +
                "ci.order_id,  " +
                "date_format(ci.create_time,'yyyy-MM-dd') date_id,  " +
                "ci.create_time,  " +
                "ci.appraise,  " +
                "dic.dic_name,  " +
                "cast(ts as string) ts  " +
                "from comment_info ci " +
                "join base_dic for system_time as of ci.pt as dic on ci.appraise=dic.dic_code ");

        // 5. 写出到kafka
        tEnv.executeSql("create table dwd_interaction_comment( " +
                "id string,  " +
                "user_id string,  " +
                "sku_id string,  " +
                "order_id string,  " +
                "date_id string,  " +
                "create_time string,  " +
                "appraise_code string,  " +
                "appraise_name string,  " +
                "ts string  " +
                ")" + SQLUtil.getKafkaSinkDDL(Constant.KAFKA_TOPIC_DWD_INTERACTION_COMMENT));

        result.executeInsert("dwd_interaction_comment");
    }
}
