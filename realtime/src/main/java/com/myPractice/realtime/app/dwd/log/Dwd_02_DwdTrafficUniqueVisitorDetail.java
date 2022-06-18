package com.myPractice.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.DateFormatUtil;
import com.myPractice.realtime.util.MyUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/17 20:02
 *
 * 流量域独立访客事务事实表 UV
 *  UV任务：过滤数据，一个用户只保留当天的第一条访问记录
 *
 *  数据源
 *      启动日志？
 *          可以，记录会偏小
 *              通过app访问才有启动日志
 *              通过浏览器访问的不记录
 *      页面日志————选这个
 *
 *  去重逻辑
 *      用一个状态保存这个用户的最后一次访问的日期
 *
 *      状态是null
 *          第一次启动，这条记录保留，然后更新状态为当前日期
 *
 *      状态不为null
 *          状态和当前日期相等
 *              数据丢弃，不进行任何操作
 *
 *          状态和当前日期不相等
 *              变到了第二天，这条数据是第二天的第一条访问记录，保留，更新状态为第二天日期
 *
 *      考虑乱序问题：
 *          事件时间+水印
 *
 *          找到当天的第一个窗口
 *              这个窗口内的数据按照时间排序，取时间最小的那条数据取出
 *
 *          其他窗口忽略
 */
public class Dwd_02_DwdTrafficUniqueVisitorDetail extends BaseAppV1 {
    public static void main(String[] args) {
        // 创建执行环境 选用启动日志所以消费的topic是dwd_traffic_page
        new Dwd_02_DwdTrafficUniqueVisitorDetail().init(10000, 2, "Dwd_02_DwdTrafficUniqueVisitorDetail", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream
                // 先转换成json对象
                .map(JSON::parseObject)
                // 取出时间戳作为水印
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((e, c) -> e.getLong("ts"))
                )
                // 使用用户id进行分组
                .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
                // 使用窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {

                    private ValueState<String> firstWindowState;

                    @Override
                    public void open(Configuration parameters) {
                        firstWindowState = getRuntimeContext().getState(new ValueStateDescriptor<>("firstWindowState", String.class));
                    }

                    @Override
                    public void process(String key, Context context, Iterable<JSONObject> elements, Collector<String> out) throws Exception {
                        String yesterday = firstWindowState.value();
                        String today = DateFormatUtil.toDate(context.window().getStart());

                        // 一定要把today写前面，避免空指针
                        if (!today.equals(yesterday)){
                            // 变成了第二天，则先更新状态
                            firstWindowState.update(today);

                            // 取出窗口内所有元素
                            List<JSONObject> list = MyUtil.toList(elements);

                            // 选出最小的时间
                            JSONObject min = Collections.min(list, Comparator.comparing(obj -> obj.getLong("ts")));

                            out.collect(JSONObject.toJSONString(min));
                        }
                    }
                })
                // 将一个用户当天最早的一条访问记录发送到kafka
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UV));


    }
}
