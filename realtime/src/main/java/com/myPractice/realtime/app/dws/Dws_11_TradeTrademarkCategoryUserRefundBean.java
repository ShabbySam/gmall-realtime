package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.TradeTrademarkCategoryUserRefundBean;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.function.DimAsyncFunction;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/30 19:12
 *
 * 交易域品牌-品类-用户粒度退单各窗口汇总表
 *  from 退单表 dwd_trade_order_refund
 *  粒度 品牌-品类-用户
 *   不用去重，没有跟预处理表相关
 */
public class Dws_11_TradeTrademarkCategoryUserRefundBean extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_11_TradeTrademarkCategoryUserRefundBean().init(
            3111,
            2,
            "Dws_11_TradeTrademarkCategoryUserRefundBean",
            Constant.DWD_TRADE_ORDER_REFUND
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 解析成pojo类型
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream = parseToPojo(stream);

        // 聚合的key需要C3ID和TmID
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3IdAndTmIdStream = joinC3IdAndTmId(beanStream);

        // 2. 开窗聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> aggregatedStream = windowAndAgg(c3IdAndTmIdStream);
        //aggregatedStream.print();

        // 3. 补充维度信息
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultStream = addDim(aggregatedStream);
        resultStream.print();

        // 4. 写入到clickhouse
        writeToClickHouse(resultStream);
    }

    private void writeToClickHouse(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_trademark_category_user_refund_window", TradeTrademarkCategoryUserRefundBean.class));
    }


    /**
     * 添加维度数据
     * @param aggregatedStream 聚合后的数据流
     * @return 补充维度后的数据流
     */
    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> addDim(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> aggregatedStream) {
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmStream = AsyncDataStream.unorderedWait(
                aggregatedStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public String getTable() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dim) {
                        input.setTrademarkName(dim.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3Stream = AsyncDataStream.unorderedWait(
                tmStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public String getTable() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dim) {
                        input.setCategory3Name(dim.getString("NAME"));
                        input.setCategory2Id(dim.getString("CATEGORY2_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public String getTable() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dim) {
                        input.setCategory2Name(dim.getString("NAME"));
                        input.setCategory1Id(dim.getString("CATEGORY1_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        return AsyncDataStream.unorderedWait(
                c2Stream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public String getTable() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dim) {
                        input.setCategory1Name(dim.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
    }

    /**
     * 开窗聚合
     * @param beanStream 解析成pojo类型的数据流
     * @return 聚合后的数据流
     */
    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> windowAndAgg(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .keyBy(bean -> bean.getTrademarkId() + ":" + bean.getCategory3Id() + ":" + bean.getUserId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<TradeTrademarkCategoryUserRefundBean> elements, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                                TradeTrademarkCategoryUserRefundBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.toYmdHms(context.window().getStart()));
                                bean.setEdt(DateFormatUtil.toYmdHms(context.window().getEnd()));

                                bean.setRefundCount((long) bean.getOrderIdSet().size());

                                bean.setTs(context.currentProcessingTime());

                                out.collect(bean);
                            }
                        }
                );
    }


    /**
     * 聚合的key需要C3ID和TmID,先进行维度获取
     * @param beanStream
     * @return
     */
    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> joinC3IdAndTmId(SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream) {
        return AsyncDataStream.unorderedWait(
                beanStream,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getId(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getSkuId();
                    }

                    @Override
                    public String getTable() {
                        return "dim_sku_info";
                    }

                    @Override
                    public void addDim(TradeTrademarkCategoryUserRefundBean input, JSONObject dim) {
                        input.setTrademarkId(dim.getString("TM_ID"));
                        input.setCategory3Id(dim.getString("CATEGORY3_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
    }


    /**
     * 解析成pojo类型
     * @param stream 数据源
     * @return 解析后的数据流
     */
    private SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> parseToPojo(DataStreamSource<String> stream) {
        return stream.map(new MapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public TradeTrademarkCategoryUserRefundBean map(String value) throws Exception {
                JSONObject obj = JSON.parseObject(value);
                return TradeTrademarkCategoryUserRefundBean.builder()
                        .skuId(obj.getString("sku_id"))
                        .userId(obj.getString("user_id"))
                        .orderIdSet(new HashSet<>(Collections.singletonList(obj.getString("order_id"))))
                        .ts(obj.getLong("ts") * 1000)
                        .build();
            }
        });
    }
}
