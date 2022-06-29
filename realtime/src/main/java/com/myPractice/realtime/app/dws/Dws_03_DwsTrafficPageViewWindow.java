package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.TrafficHomeDetailPageViewBean;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
 * @create 2022/6/27 1:29
 *
 *
 * 流量域页面浏览各窗口汇总表
 *  需求：首页/商品详情页的UV
 *      没有维度->全窗口，创建一个bean，来一条数据是首页就首页+1，商品详情页就商品详情页+1，都不是就不处理
 *          一个用户可能一天多次访问首页，需要去重，用状态
 *  from：页面日志(流量域页面浏览事务事实表) dwd_traffic_page
 *
 *
 */
public class Dws_03_DwsTrafficPageViewWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_03_DwsTrafficPageViewWindow()
                .init(4396,
                        2,
                        "Dws_03_DwsTrafficPageViewWindow",
                        Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        // 1. 先找到每个用户 每天的一条首页记录和详情记录
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = findUv(stream);

        // 2. 开窗聚合：没有keyBy，全窗口聚合函数
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream = windowAndAggregate(beanStream);


        // 3. 写出clickhouse中
        writeToClickHouse(resultStream);


    }

    private void writeToClickHouse(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHouseSink("dws_traffic_page_view_window", TrafficHomeDetailPageViewBean.class));
    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> windowAndAggregate(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> stream) {
        return stream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean bean1,
                                                                        TrafficHomeDetailPageViewBean bean2) throws Exception {
                                bean1.setHomeUvCt(bean1.getHomeUvCt()+bean2.getHomeUvCt());
                                bean1.setGoodDetailUvCt(bean1.getGoodDetailUvCt()+bean2.getGoodDetailUvCt());

                                return bean1;
                            }
                        },
                        new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window,
                                              Iterable<TrafficHomeDetailPageViewBean> it,
                                              Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                                TrafficHomeDetailPageViewBean bean = it.iterator().next();

                                bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                                bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));

                                bean.setTs(System.currentTimeMillis());
                                System.out.println(bean);
                                out.collect(bean);

                            }
                        }
                );

    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> findUv(DataStreamSource<String> stream) {
        return stream
                // 先把字符串解析橙JSON格式
                .map(JSON::parseObject)
                .filter(obj -> {
                    String pageId = obj.getJSONObject("page").getString("page_id");
                    return "home".equals(pageId) || "good_detail".equals(pageId);
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj,ts)->obj.getLong("ts"))
                )
                .keyBy(obj->obj.getJSONObject("common").getString("mid")) // 同一个用户的数据放一起
                .process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

                    private ValueState<String> detailState;
                    private ValueState<String> homeState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 一个存home页面的今天日期用于判断是不是今天，一个存good detail页面今天日期用于判断是不是今天
                        homeState = getRuntimeContext().getState(new ValueStateDescriptor<String>("homeState", String.class));
                        detailState = getRuntimeContext().getState(new ValueStateDescriptor<String>("goodDetailState", String.class));

                    }

                    @Override
                    public void processElement(JSONObject obj, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        String pageId = obj.getJSONObject("page").getString("page_id");

                        // 通过水印获得今日日期
                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.toDate(obj.getLong("ts"));

                        Long homeUvCt = 0L;
                        Long goodDetailUvCt = 0L;

                        // 判断是否是今天第一个首页来了
                        if ("home".equals(pageId) && !today.equals(homeState.value())){
                            homeUvCt = 1L;
                            homeState.update(today);
                        // 判断是否是今天第一个商品详情页来了
                        } else if ("good_detail".equals(pageId) && !today.equals(detailState.value())){
                            goodDetailUvCt = 1L;
                            detailState.update(today);
                        }

                        if ( homeUvCt + goodDetailUvCt == 1){
                            out.collect(new TrafficHomeDetailPageViewBean("", "", homeUvCt, goodDetailUvCt, ts));
                        }
                    }
                });
    }
}
