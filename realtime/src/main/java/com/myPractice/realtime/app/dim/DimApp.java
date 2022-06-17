package com.myPractice.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.TableProcess;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.FlinkSourceUtil;
import com.myPractice.realtime.util.JdbcUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/14 16:14
 */
public class DimApp extends BaseAppV1 {
    /**
     * data from: ods_db
     *      很多的维度信息
     *
     *
     * 如何把不同的维度表写入到Phoenix中的表
     *  1. Phoenix中的表如何创建
     *      a：手动创建
     *          简单，提前根据需要把表建好
     *
     *          不灵活：没有办法根据维度表的变化实时适应新的变化
     *
     *      b：动态创建
     *          提前做好一个配置表，配置表中配置了所有需要的维度信息
     *          flink程序会根据配置信息自动的创建对应的表
     *
     *
     *  2. 维度数据如何写入到Phoenix中
     *      根据配置信息来决定这条数据应该写入到什么表中
     *
     * ------------------------------------------------------------------
     * phoenix的盐表：hbase中的预分区
     * region
     *      自动分裂
     *      自动迁移
     * 一般禁止自动分裂
     *
     */
    public static void main(String[] args) {

        new DimApp().init(10000, 2, "DimApp", Constant.TOPIC_ODS_DB);

    }


    /**
     * 写具体业务，对流数据进行处理
     * @param env: 执行流程序的环境
     * @param stream：创建的流
     */
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 对数据做etl数据清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(stream);

        // 2. 读取配置表的数据：flink CDC  后续做成广播流
        SingleOutputStreamOperator<TableProcess> tpStream = ReadTableProcess(env);

        // 3. 数据流和广播流做connect
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream = connect(etlStream, tpStream);

        // 4. 过滤掉不需要的列
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filteredStream = filterNotNeedColumns(connectedStream);
        filteredStream.print();

        // 5. 根据配置信息把数据写入到不同的Phoenix表中
        writeToPhoenix(filteredStream);

    }






    /**
     * 写入到Phoenix中
     * @param filteredStream：过滤后的数据流
     */
    private void writeToPhoenix(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filteredStream) {
        /*
         * 自定义sink；
         *     1. 能不能使用jdbc sink？
         *      不能，因为jdbc sink只能写入一个表，我们有多个表
         *     2. 自定义sink
         */
        filteredStream.addSink(FlinkSinkUtil.getPhoenixSink());
    }


    /**
     * 聚合后通过与广播流中的配置信息作对比，删除数据流中不需要的列
     * @param connectedStream：数据流与广播流connect生成的流，
     *                          每一条数据都是一个Tuple2<JSONObject, TableProcess>，
     *                             其中JSONObject是数据，TableProcess是配置信息
     * @return 返回过滤后的数据流
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> filterNotNeedColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedStream) {
        return connectedStream
                .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>() {
                         @Override
                         public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> obj) throws Exception {
                             JSONObject data = obj.f0;
                             TableProcess tp = obj.f1;

                             // 取出配置表中列名那一列
                             List<String> tpColumns = Arrays.asList(tp.getSinkColumns().split(","));
                             // 取出数据中所有的key，key其实就是数据中的列名
//                            Set<String> dataColumns = data.keySet();

                             // 如果数据中的列名在配置表中不存在，则删除这个列
                             data.keySet().removeIf(key -> !tpColumns.contains(key));

                             return obj;
                         }
                     }
                );
    }





    /**
     * 数据流与广播流做connect
     * @param dataStream : 数据流
     * @param tpStream : 需要做成广播流的流
     * @return 返回connect后的数据流
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<JSONObject> dataStream, SingleOutputStreamOperator<TableProcess> tpStream) {
        // 1. 把配置流做成广播流
        /*
        keys：mysql中的表名
        value：TableProcess对象
         */

        MapStateDescriptor<String, TableProcess> tpStateDesc = new MapStateDescriptor<>("tpState", String.class, TableProcess.class);
        BroadcastStream<TableProcess> bcStream = tpStream.broadcast(tpStateDesc);

        // 2. 数据流去connect广播流
        return dataStream
                .connect(bcStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject,TableProcess>>() {

                    private Connection conn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 建立Phoenix的连接
                        conn = JdbcUtil.getPhoenixConnection();

                    }

                    @Override
                    public void close() throws Exception {
                        // 关闭Phoenix的连接
                        if (conn != null) {
                            conn.close();
                        }
                    }

                    // 处理业务数据
                    @Override
                    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        // 4. 处理业务数据：从广播状态中读取配置信息，吧数据和配置组成一队，交给后续的流进行处理
                        TableProcess tp = ctx.getBroadcastState(tpStateDesc).get(value.getString("table"));
                        if (tp != null) { // 先判断配置信息是否为null 如果为null, 不需要: 可能是配置表没写的不需要的维度表,或者是事实表.
                            out.collect(Tuple2.of(value.getJSONObject("data"), tp));// 有用的信息只剩下data
                        }
                    }

                    // 处理广播流的数据
                    @Override
                    public void processBroadcastElement(TableProcess tp, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        // 1. 处理广播数据：把广播数据写入广播状态
                        saveTpToState(tp, ctx);

                        // 2. 根据配置信息，在Phoenix中创建对应的表，但这里建表会因为并行度的设置导致多次重复建表，最好是在广播前建表，只要有tp数据就能建表
                        createTable(tp);
                    }

                    private void createTable(TableProcess tp) throws SQLException {
                        // 去Phoenix中创建表：jdbc
                        // 1. 拼接sql语句
                        // create table if not exists table_name (col1 varchar, col2 varchar, col3 varchar, constraint pk primary key(col1))SALT_BUCKETS=3;
                        StringBuilder sql = new StringBuilder("create table if not exists ");
                        sql.append(tp.getSinkTable());
                        sql.append(" (");
                        for (String col : tp.getSinkColumns().split(",")) {
                            sql.append(col);
                            sql.append(" varchar, ");
                        }
                        sql.append(" constraint pk primary key(");
                        sql.append(tp.getSinkPk() == null ? "id" : tp.getSinkPk());
                        sql.append("))");
                        sql.append(tp.getSinkExtend() == null ? "" : tp.getSinkExtend());

                        System.out.println("建表语句: " + sql);

                        // 2. 通过sql语句，得到一个预处理语句：PreparedStatement
                        PreparedStatement ps = conn.prepareStatement(sql.toString());

                        // 3. 给占位符赋值，这里没有占位符，所以不需要

                        // 4. 执行预处理语句
                        ps.execute();
                        // 5. 关闭预处理语句
                        ps.close();
                    }

                    /**
                     * 把广播流的数据写入广播状态
                     * @param tp: 广播流的数据
                     * @param ctx: 上下文环境
                     * @throws Exception
                     */
                    private void saveTpToState(TableProcess tp, Context ctx) throws Exception {
                        BroadcastState<String, TableProcess> state = ctx.getBroadcastState(tpStateDesc);
                        state.put(tp.getSourceTable(), tp);
                    }
                });
    }





    /**
     * 读取配置表数据
     *      在MySQL中的gmall_config -> table_process表中
     *
     *          先读取全量数据，然后读取变化的数据
     *          传统的方式：MySQL->Maxwell->Kafka->Flink
     *          新的方式：Flink CDC
     *
     * @param env ：执行流程序的环境
     * @return : 带有配置表信息的广播流
     */
    private SingleOutputStreamOperator<TableProcess> ReadTableProcess(StreamExecutionEnvironment env) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop162")
                .port(3306)
                .scanNewlyAddedTableEnabled(false) // eanbel scan the newly added tables fature
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process") // set captured tables [product, user, address]
                .username("root")
                .password("aaaaaa")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"cdc-source")
                .map(json -> {
                    JSONObject jsonObject = JSON.parseObject(json);
                    return jsonObject.getObject("after", TableProcess.class);
                });
    }




    /**
     * 清洗数据
     * @param stream ：读取kafka数据创建的流
     * @return ：清洗后的流
     */
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter((FilterFunction<String>) input -> {
                    try {
                        JSONObject obj = JSON.parseObject(input);

                        String type = obj.getString("type");
                        JSONObject data = obj.getJSONObject("data");

                        return "gmall2022".equals(obj.getString("database"))
                                && ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type))
                                && data != null;

                    } catch (Exception e) {
                        System.out.println("数据格式不是Json");
                        return false;
                    }
                })
                .map(JSON::parseObject);
    }
}
