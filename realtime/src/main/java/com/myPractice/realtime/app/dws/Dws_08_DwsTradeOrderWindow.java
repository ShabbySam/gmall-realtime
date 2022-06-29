package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.TradeOrderBean;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/27 21:08
 *
 * 交易域下单各窗口汇总表
 *  从 Kafka 订单明细主题读取数据，对数据去重，统计当日下单独立用户数、新增下单用户数、下单原始金额、优惠券减免金额和活动减免金额，封装为实体类，写入 ClickHouse。
 *
 */
public class Dws_08_DwsTradeOrderWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_08_DwsTradeOrderWindow().init(
                4396,
                2,
                "Dws_08_DwsTradeOrderWindow",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                    private ValueState<String> lastOrderDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastOrderDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastOrderDateState", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TradeOrderBean>.Context ctx, Collector<TradeOrderBean> out) throws Exception {
                        long ts = value.getLong("ts") * 1000;
                        String today = DateFormatUtil.toDate(ts);

                        String lastOrderDate = lastOrderDateState.value();

                        long uuCt = 0;
                        long uuCtNew = 0;

                        if (!today.equals(lastOrderDate)) {
                            // 表示今天第一次下单
                            uuCt = 1;
                            lastOrderDateState.update(today);

                            // 如果状态是null说明没下单，就是新用户
                            if (lastOrderDate == null) {
                                uuCtNew = 1;
                            }
                        }

                        if (uuCt == 1) {
                            out.collect(new TradeOrderBean(
                                    "",
                                    "",
                                    uuCt,
                                    uuCtNew,
                                    ts));
                        }

                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                                return value1;
                            }
                        },
                        new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                                TradeOrderBean bean = values.iterator().next();
                                bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                                bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                                bean.setTs(System.currentTimeMillis());

                                out.collect(bean);
                            }
                        })
                .addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_order_window", TradeOrderBean.class));
    }
}
