package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.TradeSkuOrderBean;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.function.DimAsyncFunction;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/28 10:06
 *
 *
 * 交易域用户-SPU粒度下单各窗口汇总表
 *  来源：订单详情表
 *      详情表来源：预处理表
 *          预处理表中有左连接 -> 有重复数据 -> 去重
 *  去重
 *      有一条详情数据出现大量重复
 *          粒度：sku
 *              detail_1  sku_1   100    null（右表还没来，就先null）
 *              detail_1  sku_1   100     10 （右表来了）
 *          去重的时候按照detail_1去重，一个sku可能有多个用户购买，所以不能用sku去重
 *              保留最后一条数据 -> 时间戳最大
 *
 *              1. 如何找到最后一条：
 *                  详情表 left join 详情活动 left join 详情优惠券
 *                  5s内，如果有的话一定来齐（具体时间看各自网络）
 *                      定时器：
 *                          第一条来的时候注册定时器
 *                              中间每来一条数据就判断时间，如果来的时间大于状态中存储的那个时间，更新
 *                          等定时器触发，最后一条一定来了
 *
 *              2. 去重简化：
 *                  为什么一定要用最后一条？因为最后一条的数据比较完整
 *                      如果在统计计算的时候根本用不到右表数据，这种情况只要第一条就行
 *
 *              3. 用窗口 session
 *                  gap 5秒
 *                  当窗口关闭的时候，同一个详情的数据一定都到了，再从这个窗口内找到操作时间最大那个
 *                      时效性较低
 *
 *
 *  按照ku分组，开窗，聚合
 *
 *  补充维度信息：详情中只有一个sku_id
 *      sku_name
 *       spu
 *       tm
 *       ...
 *
 *  最后写入到clickhouse中
 *
 */
public class Dws_09_DwsTradeSkuOrderWindow_Cache_Async extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow_Cache_Async().init(
                4396,
                2,
                "dws_09_dwsTradeSkuOrderWindow",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 先找order_detail_id进行去重
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);

        // 2. 根据需要去除一些字段，封装到bean
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(distinctedStream);
        //beanStream.print();

        // 3. 按照sku_id进行分组开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> streamWithoutDim = windowAndAggregate(beanStream);
        //streamWithoutDim.print();

        // 4. 补充维度信息
        SingleOutputStreamOperator<TradeSkuOrderBean> streamWithDim = joinDim(streamWithoutDim);
        //streamWithDim.print();

        // 5. 写出到clickhouse中
        writeToClickHouse(streamWithDim);
    }

    private void writeToClickHouse(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        stream.addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_sku_order_window", TradeSkuOrderBean.class));
    }

    /**
     * 添加维度信息 -> 读取储存在hbase里的维度表 -> 对维度表读取多个字段的数据时可以做个异步提升效率
     * 原本：同步方式：一个一个算子下来，上一个不完事下一个不开始
     *
     *
     * 异步方式：
     *  发送请求后，不会等待返回，直接发送第二个请求。。。----->可以解决网络延迟的问题
     *
     *  多线程+多客户端（最好是异步客户端但是没有）
     *      每个线程创建一个客户端进行请求查询
     *
     *-----------------------------------------------------------------------------------------------
     *
     * 异步超时错误：
     *  肯定是其他的原因导致异步处理没有完成，所以会超时（我这里设置的是等待60s）
     *      1. 首先检查集群 redis hdfs kafka hbase
     *      2. phoenix是否正常
     *      3. 检查phoenix中的维度表是否全
     *      4. 确定每张表都有数据
     *          通过bootstrap同步一些数据
     *      5. 寄！
     *
     * hbase起不来：
     *  hdfs导致hbase出问题
     *      重置
     *          先停止
     *              hdfs：删除目录/hbase
     *              zk：deleteall /hbase
     *          再启动
     *
     * kafka出问题
     *  删除 logs/ 目录下所有文件
     *      xcall /opt/module/kafka../logs/*
     *  zk 删除节点： deleteall /kafka
     *
     */
    private SingleOutputStreamOperator<TradeSkuOrderBean> joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> skuInfoStream = AsyncDataStream.unorderedWait(
                stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {

                    @Override
                    public String getId(TradeSkuOrderBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public String getTable() {
                        return "dim_sku_info";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean input, JSONObject dim) {
                        input.setSkuName(dim.getString("SKU_NAME"));
                        input.setSpuId(dim.getString("SPU_ID"));
                        input.setTrademarkId(dim.getString("TM_ID"));
                        input.setCategory3Id(dim.getString("CATEGORY3_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> tmStream = AsyncDataStream.unorderedWait(
                skuInfoStream,
                new DimAsyncFunction<TradeSkuOrderBean>() {

                    @Override
                    public String getId(TradeSkuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public String getTable() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean input, JSONObject dim) {
                        input.setTrademarkName(dim.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> spuStream = AsyncDataStream.unorderedWait(
                tmStream,
                new DimAsyncFunction<TradeSkuOrderBean>() {

                    @Override
                    public String getId(TradeSkuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public String getTable() {
                        return "dim_spu_info";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean input, JSONObject dim) {
                        input.setSpuName(dim.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(
                spuStream,
                new DimAsyncFunction<TradeSkuOrderBean>() {

                    @Override
                    public String getId(TradeSkuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public String getTable() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean input, JSONObject dim) {
                        input.setCategory3Name(dim.getString("NAME"));
                        input.setCategory2Id(dim.getString("CATEGORY2_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {

                    @Override
                    public String getId(TradeSkuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public String getTable() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean input, JSONObject dim) {
                        input.setCategory2Name(dim.getString("NAME"));
                        input.setCategory1Id(dim.getString("CATEGORY1_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        return AsyncDataStream.unorderedWait(
                c2Stream,
                new DimAsyncFunction<TradeSkuOrderBean>() {

                    @Override
                    public String getId(TradeSkuOrderBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public String getTable() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean input, JSONObject dim) {
                        input.setCategory1Name(dim.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );


    }


    /**
     * 开窗聚合
     * @param stream 输入流
     * @return 输出流
     */
    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAggregate(
       SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1,
                                                            TradeSkuOrderBean value2) throws Exception {
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                                value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                                value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());

                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<TradeSkuOrderBean> elements,
                                                Collector<TradeSkuOrderBean> out) throws Exception {

                                TradeSkuOrderBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.toYmdHms(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.toYmdHms(ctx.window().getEnd()));

                                bean.setTs(System.currentTimeMillis());

                                // 根据set集合的长度去设置orderCount的值
                                bean.setOrderCount((long) bean.getOrderIdSet().size());

                                out.collect(bean);


                            }
                        }
                );
    }


    /**
     * 根据需要去除一些字段，封装到bean
     * @param stream 去重后的流
     * @return
     */
    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(SingleOutputStreamOperator<JSONObject> stream) {
        return stream
                .map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject value) throws Exception {
                        TradeSkuOrderBean bean = TradeSkuOrderBean.builder()
                                .skuId(value.getString("sku_id"))

                                // getDoubleValue(): 如果字段的值是null, 则会赋值0D
                                .originalAmount(value.getDoubleValue("split_original_amount"))
                                .activityAmount(value.getDoubleValue("split_activity_amount"))
                                .couponAmount(value.getDoubleValue("split_coupon_amount"))
                                .orderAmount(value.getDoubleValue("split_total_amount"))
                                .ts(value.getLong("ts") * 1000)
                                .build();

                        bean.getOrderIdSet().add(value.getString("order_id"));

                        return bean;
                    }
                });
    }


    /**
     * 先找order_detail_id进行去重
     * @param stream 从dwd_trade_order_detail获取的数据源
     */
    private SingleOutputStreamOperator<JSONObject> distinctByOrderDetailId(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> maxDateDataState;

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        // 定时器触发的时候，状态中保存的一定是时间最大的那条数据，最后一个最完整的数据
                        out.collect(maxDateDataState.value());
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        maxDateDataState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("maxDateDataState", JSONObject.class));
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (maxDateDataState.value() == null) {
                            // null，说明第一条数据进来
                            // 1. 注册定时器，5s后触发
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                            // 2. 更新状态
                            maxDateDataState.update(value);
                        } else {
                            // 不是第一条
                            // 3. 比较时间，新来的数据如果时间比较大就保存它的数据（并更新状态），注意，时间后面加了个z，写个方法去掉
                            String current = value.getString("row_op_ts");
                            String last = maxDateDataState.value().getString("row_op_ts");
                            if (MyUtil.compareLTZ(current, last)) {
                                maxDateDataState.update(value);
                            }
                        }
                    }
                });
    }
}
