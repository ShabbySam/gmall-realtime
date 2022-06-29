package com.myPractice.realtime.app.dws;

import com.myPractice.realtime.app.BaseSQLAppV1;
import com.myPractice.realtime.bean.KeywordBean;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.function.IKAnalyzer;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/24 15:38
 *
 * 流量域来源关键词粒度页面浏览各窗口汇总表
 *  1. 找到搜索关键词
 *      last_page_id = search
 *      item_type = keyword
 *      item is not null
 *          取出item
 *  2. 搜索关键
 *  “华为 128G 手机”
 *  “华为 256G 手机 白色”
 *  关键词：华为 手机
 *      统计每个关键词的次数 —> 先分词 —> elasticsearch：ik分词器 —>它的jar
 *
 *  3. 分词后，分组（单词）开窗聚合
 *  华为
 *  手机
 *  128g
 *  256g
 *  白色
 *
 *  4. 华为 0-5 10
 *  华为 5-10 20
 *  手机 0-5 2
 *  手机 5-10 3
 *  .....
 *
 *
 *  ads可视化结果展示：字符云图
 */
public class Dws_01_DwsTrafficSourceKeywordPageViewWindow extends BaseSQLAppV1 {
    public static void main(String[] args) {
        new Dws_01_DwsTrafficSourceKeywordPageViewWindow().init(
            3011,
            2,
            "Dws_01_DwsTrafficSourceKeywordPageViewWindow",
            10
        );
    }

    @Override
    protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // 1. 读取页面日志数据：ddl语句
        tEnv.executeSql("create table page_log( " +
                " page map<string,string>, " +
                " ts bigint, " +
                " et as to_timestamp_ltz(ts,3), " + // 0表示前面的ts是秒，3表示毫秒
                " watermark for et as et - interval '3' second " +
                " ) " + SQLUtil.getKafkaSourceDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE , "Dws_01_DwsTrafficSourceKeywordPageViewWindow"));


        // 2. 过滤出搜索记录(last_page_id=search...)，取出搜索关键词
        Table t1 = tEnv.sqlQuery("select " +
                " page['item'] keyword, " +
                " et " +
                " from page_log " +
                " where page['last_page_id'] = 'search' " +
                " and page['item_type'] = 'keyword' " +
                " and page['item'] is not null ");
        tEnv.createTemporaryView("t1",t1);



        /*
            分词: 没有现有函数
                自定义函数?一看就是用制表函数
                标量函数(scalar)  udf —> 一进一出
                制表函数(table)  udtf —> 一进多出
                聚合函数(aggregate) udaf —> 多进一出
                制表聚合函数(table aggregate) udaf —> 多进多出

            分词效果:
            苹果手机
                苹果
                手机

                table function

                    eval：执行
                    pubic void eval(String s){
                        collect(...)
                        collect(...)
                        collect(...)
                }
        */
        // 3. 对关键词进行分词
        tEnv.createTemporaryFunction("ik_analyzer", IKAnalyzer.class);
        Table t2 = tEnv.sqlQuery("select " +
                " kw, " +
                " et " +
                " from t1 " +
                " join lateral table(ik_analyzer(keyword)) on true ");//内连接制表函数
        tEnv.createTemporaryView("t2", t2);

        // 4. 开窗聚合：分组窗口 tvf over 选tvf
        // 分组窗口：滚动 滑动 会话
        // tvf:滚动 滑动 累计
        Table resultTable = tEnv.sqlQuery("select " +
                " date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt, " +
                " date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt, " +
                " 'search' source, " +
                " kw keyword, " +
                " count(*) keywordCount, " +
                " unix_timestamp() * 1000 as ts " +
                "from table( tumble( table t2, descriptor(et), interval '5' second ) ) " +
                "group by kw, window_start, window_end");


        // 5. 把结果写出到clickhouse中，但是flink没有提供clickhouse的连接器，只能自己写代码
        /*
            自定义连接器——>自定义流到sink
            clickhouse是标准的数据库，支持JDBC连接，所以在JDBC连接的基础上封装一个连接器
            bean里面的字段名要和表中的字段名保持一致, 这样才能使用反射的反射方式写入
         */
        // toRetractStream：可以用变化流（比如里面有聚合）
        // toAppendStream：只能追加数据，不能变化
        tEnv
                .toRetractStream(resultTable, KeywordBean.class)
                // 只要新增的数据，就会输出，删除的数据，不会输出
                // 数据形态是一个tuple2数组 f0是Boolean值，新的为true 旧的为false，所以选true  以f0作为筛选条件
                .filter(t -> t.f0)
                // 布尔值数据不需要的，只取f1
                .map(t -> t.f1)
                .addSink(FlinkSinkUtil.getClickHouseSink("dws_traffic_source_keyword_page_view_window",KeywordBean.class ));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

