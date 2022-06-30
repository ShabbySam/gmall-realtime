package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.TradeProvinceOrderWindow;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.function.DimAsyncFunction;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.DateFormatUtil;
import com.myPractice.realtime.util.MyUtil;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/30 11:38
 *
 *
 * 交易域省份粒度下单各窗口汇总表
 *  from 订单明细表 dwd_trade_order_detail
 *  统计各省份各窗口订单数和订单金额
 */
public class Dws_10_DwsTradeProvinceOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_10_DwsTradeProvinceOrderWindow().init(
            3110,
            2,
            "Dws_10_DwsTradeProvinceOrderWindow",
            Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 按照orderdetail_id去重
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);

        // 2. 解析成pojo类型
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStream = parseToPojo(distinctedStream);

        // 3. 开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindow> aggregatedStream = windowAndAggregate(beanStream);
        //aggregatedStream.print();

        // 4. 补充维度信息
        SingleOutputStreamOperator<TradeProvinceOrderWindow> resultStream = joinDim(aggregatedStream);
        //resultStream.print();

        // 5. 写入到clickhouse
        writeToClickhouse(resultStream);
    }

    private void writeToClickhouse(SingleOutputStreamOperator<TradeProvinceOrderWindow> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_province_order_window", TradeProvinceOrderWindow.class));
    }

    /**
     * 从dim_province表中获取省份维度信息
     * @param aggregatedStream 已经开窗聚合的stream
     * @return
     */
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> joinDim(SingleOutputStreamOperator<TradeProvinceOrderWindow> aggregatedStream) {
        return AsyncDataStream.unorderedWait(
                aggregatedStream,
                new DimAsyncFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public String getId(TradeProvinceOrderWindow input) {
                        return input.getProvinceId();
                    }

                    @Override
                    public String getTable() {
                        return "dim_base_province";
                    }

                    @Override
                    public void addDim(TradeProvinceOrderWindow input, JSONObject dim) {
                        input.setProvinceName(dim.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
    }


    /**
     * 开窗聚合
     * @param stream 解析成pojo类型的流
     * @return 聚合后的流
     */
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> windowAndAggregate(SingleOutputStreamOperator<TradeProvinceOrderWindow> stream) {
        return stream
                .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((bean, ts)->bean.getTs())
        )
                .keyBy(TradeProvinceOrderWindow::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
                            @Override
                            public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context ctx, Iterable<TradeProvinceOrderWindow> elements, Collector<TradeProvinceOrderWindow> out) throws Exception {
                                TradeProvinceOrderWindow bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.toYmdHms(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.toYmdHms(ctx.window().getEnd()));

                                bean.setOrderCount((long) bean.getOrderIdSet().size());

                                bean.setTs(ctx.currentProcessingTime());

                                out.collect(bean);
                            }
                        });
    }

    /**
     * 解析成pojo类型
     * @param stream 原始数据流
     * @return pojo类型数据流
     */
    private SingleOutputStreamOperator<TradeProvinceOrderWindow> parseToPojo(SingleOutputStreamOperator<JSONObject> stream) {
        return stream.map(new MapFunction<JSONObject, TradeProvinceOrderWindow>() {
            @Override
            public TradeProvinceOrderWindow map(JSONObject value) throws Exception {
                // 用builder注解给的方法
                return TradeProvinceOrderWindow.builder()
                        .provinceId(value.getString("province_id"))
                        .orderIdSet(new HashSet<>(Collections.singleton(value.getString("order_id"))))
                        .orderAmount(value.getDoubleValue("split_total_amount"))
                        .ts(value.getLong("ts") * 1000)
                        .build();
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
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
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
