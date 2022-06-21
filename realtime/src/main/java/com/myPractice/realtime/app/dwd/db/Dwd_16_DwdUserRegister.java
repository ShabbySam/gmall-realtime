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
 * @create 2022/6/21 21:41
 *
 * 用户域用户注册事务事实表
 *  user_info
 *      用户注册一次，新增一条数据-------操作类型为 insert，
 */
public class Dwd_16_DwdUserRegister extends BaseSQLAppV1 {
    public static void main(String[] args) {
        new Dwd_16_DwdUserRegister().init(
                2011,
                2,
                "Dwd_16_DwdUserRegister",
                10
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取ods_db数据
        readOdsDb(tEnv, "Dwd_16_DwdUserRegister");
        // 2. 过滤出来用户表
        Table userInfo = tEnv.sqlQuery("select " +
                "data['id'] user_id,  " +
                "date_format(data['create_time'], 'yyyy-MM-dd' ) date_id,  " +
                "data['create_time'] create_time,  " +
                "cast(ts as string) ts  " +
                " from ods_db" +
                " where `database`='gmall2022' " +
                " and `table`='user_info' " +
                " and `type`='insert'");
        // 3. 写出到kafka
        tEnv.executeSql("create table dwd_user_register (" +
                "user_id string,  " +
                "date_id string,  " +
                "create_time string,  " +
                "ts string  " +
                ") " + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_USER_REGISTER));
        userInfo.executeInsert("dwd_user_register");
    }
}
