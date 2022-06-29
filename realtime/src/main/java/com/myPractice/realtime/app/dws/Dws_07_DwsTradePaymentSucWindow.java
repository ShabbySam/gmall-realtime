package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.TradePaymentWindowBean;
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
 * @create 2022/6/27 20:29
 *
 *
 * 交易域支付各窗口汇总表
 *  统计支付成功独立用户数和首次支付成功用户数
 *  from 交易域支付成功主题数据 dwd_trade_refund_pay_suc
 */
public class Dws_07_DwsTradePaymentSucWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_07_DwsTradePaymentSucWindow().init(
                4396,
                2,
                "dws_07_dws_trade_payment_suc_window",
                Constant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //stream.print();
        // 1. 创建窗口
        stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {

                    private ValueState<String> lastPaySucDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastPaySucDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastPaySucDateState", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>.Context ctx, Collector<TradePaymentWindowBean> out) throws Exception {
                        long ts = value.getLong("ts") * 1000;
                        String today = DateFormatUtil.toDate(ts);
                        String lastPaySucDate = lastPaySucDateState.value();

                        long uuCt = 0;
                        long newUuCt = 0;

                        if (!today.equals(lastPaySucDate)) {
                            uuCt = 1;
                            lastPaySucDateState.update(today);
                            // 这是今天的第一次支付, 然后需要判断下这个用户是否为新用户的次第一次支付
                            if (lastPaySucDate == null) {
                                newUuCt = 1;
                            }
                        }
                        if (uuCt == 1) {
                            out.collect(new TradePaymentWindowBean(
                                    "",
                                    "",
                                    uuCt,
                                    newUuCt,
                                    ts
                            ));
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradePaymentWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((e, ts) -> e.getTs())
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                            @Override
                            public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                                value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                                value1.setPaymentNewUserCount(value1.getPaymentNewUserCount() + value2.getPaymentNewUserCount());
                                return value1;
                            }
                        },
                        new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {
                                TradePaymentWindowBean bean = values.iterator().next();
                                bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                                bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                                bean.setTs(System.currentTimeMillis());

                                out.collect(bean);
                            }
                        })
                .addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_payment_suc_window", TradePaymentWindowBean.class));

    }
}
