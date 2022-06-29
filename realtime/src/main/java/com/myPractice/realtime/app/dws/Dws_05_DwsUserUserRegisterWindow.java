package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.UserRegisterBean;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * @create 2022/6/27 19:42
 *
 *
 * 用户域用户注册各窗口汇总表
 *  从 DWD 层用户注册表中读取数据，统计各窗口注册用户数，写入 ClickHouse
 *  读dwd_user_register，存到bean，写clickhouse
 */
public class Dws_05_DwsUserUserRegisterWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_05_DwsUserUserRegisterWindow().init(
                4396,
                2,
                "Dws_05_DwsUserUserRegisterWindow",
                Constant.TOPIC_DWD_USER_REGISTER
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //stream.print();
        // 转json，存bean，设置水印，开窗，聚合，写入clickhouse
        stream
                .map(json -> new UserRegisterBean("", "", 1L, JSONObject.parseObject(json).getLong("ts") * 1000))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                            @Override
                            public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                                value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                                return value1;
                            }
                        },
                        new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                                UserRegisterBean bean = values.iterator().next();

                                bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                                bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                                bean.setTs(System.currentTimeMillis());

                                out.collect(bean);
                            }
                        })
                .addSink(FlinkSinkUtil.getClickHouseSink("dws_user_user_register_window", UserRegisterBean.class));
    }
}
