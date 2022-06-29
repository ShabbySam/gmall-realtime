package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.CartAddUuBean;
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
 * @create 2022/6/27 20:02
 *
 * 交易域加购各窗口汇总表
 * 从 DWD 层加购表dwd_trade_cart_add中读取数据，统计各窗口加购数，写入 ClickHouse
 */
public class Dws_06_DwsTradeCartAddUuWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_06_DwsTradeCartAddUuWindow().init(
                4396,
                2,
                "Dws_06_DwsTradeCartAddUuWindow",
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
        // 转json，存bean，keyBy用户，挑选今日首次加购的用户，开窗聚合，写入ClickHouse
        stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {

                    private ValueState<String> lastAddCartDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastAddCartDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAddCartDateState", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, CartAddUuBean>.Context ctx, Collector<CartAddUuBean> out) throws Exception {
                        long ts = value.getLong("ts") * 1000;
                        String today = DateFormatUtil.toDate(ts);

                        if (!today.equals(lastAddCartDateState.value())) {
                            out.collect(new CartAddUuBean(
                                    "",
                                    "",
                                    1L,
                                    ts
                            ));
                            lastAddCartDateState.update(today);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                            @Override
                            public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                                value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                                return value1;
                            }
                        },
                        new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                                CartAddUuBean bean = values.iterator().next();

                                bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                                bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                                bean.setTs(System.currentTimeMillis());

                                out.collect(bean);
                            }
                        })
                .addSink(FlinkSinkUtil.getClickHouseSink("dws_trade_cart_add_uu_window", CartAddUuBean.class));
    }
}
