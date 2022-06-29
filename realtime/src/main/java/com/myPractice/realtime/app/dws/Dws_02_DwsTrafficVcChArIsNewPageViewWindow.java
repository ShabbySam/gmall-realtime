package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV2;
import com.myPractice.realtime.bean.TrafficPageViewBean;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.DateFormatUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Map;

import static com.myPractice.realtime.common.Constant.*;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/25 15:19
 *
 *
 * 流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
 *  不同的维度：
 *      会话数：页面日志
 *      页面浏览数：页面日志 pv
 *      浏览总时长：页面日志
 *      独立访客数：uv详情表 uv
 *      跳出会话数：uj详情表 uj
 *          dwd_traffic_page   dwd_traffic_uv  dwd_traffic_uj_detail
 *----------------------------------------------------------------------------------
 *  聚合结果：
 *  维度      窗口      pv      uv      uj      浏览总时长
 *  小米...   0-5      10      3       1       100
 *  华为...   0-5      20      13      3       200
 *  vivo...   0-5      10      3       1       100
 *  oppo...   0-5      20      13      3       200
 *-------------------------------------------------------------------------------
 * 从结果逆推过程
 *  聚合：按维度开窗聚合 keyBy().window().reduce()...
 *      页面浏览数：页面日志 pv
 *      独立访客数：uv详情表 uv
 *      跳出会话数：uj详情表 uj
 *
 *  一行就是进来的一条数据，只来源于某一条日志
 *      维度       pv  uv  uj
 *      小米..     1   0   0
 *      小米..     0   1   0
 *      小米..     1   0   0
 *  那么只需要全部进行聚合，就是求和
 *  并且数据要么来自页面日志，要么来自uv详情表，要么来自uj详情表，所以每条数据pv、uv、uj只有一个有有效值
 *  如果数据是pv，那么pv设为1其余为0，如果来自uv，那么uv设为1其余为0，uj同理
 *------------------------------------------------------------------------------
 * 那么上面那个表怎么来
 *  3个流union
 *
 *
 */
public class Dws_02_DwsTrafficVcChArIsNewPageViewWindow extends BaseAppV2 {
    public static void main(String[] args) {
        new Dws_02_DwsTrafficVcChArIsNewPageViewWindow().init(
                4396,
                2,
                "Dws_02_DwsTrafficVcChArIsNewPageViewWindow",
                TOPIC_DWD_TRAFFIC_PAGE,
                TOPIC_DWD_TRAFFIC_UV,
                TOPIC_DWD_TRAFFIC_UJ_DETAIL
        );
    }


    @Override
    public void handle(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> streams) {
        // 1. 解析，把多个流union成一个流
        DataStream<TrafficPageViewBean> beanStream = unionOne(streams);

        // 2. 开窗聚合
        SingleOutputStreamOperator<TrafficPageViewBean> resultStream = windowAndAggregate(beanStream);

        // 3. 写出到clickhouse
        writeToClickHouse(resultStream);


    }

    /**
     * 写出到clickhouse的方法
     * @param resultStream 聚合后的流
     */
    private void writeToClickHouse(SingleOutputStreamOperator<TrafficPageViewBean> resultStream) {
        resultStream.addSink(
                FlinkSinkUtil.getClickHouseSink("dws_traffic_vc_ch_ar_is_new_page_view_window", TrafficPageViewBean.class)
        );
    }

    /**
     * 版本-渠道-地区-访客类别粒度：多个维度需要keyBy -> 加窗口 -> 基于事件时间，加水印
     * @param stream: union后的流
     * @return 开窗聚合后的流
     */
    private SingleOutputStreamOperator<TrafficPageViewBean> windowAndAggregate(DataStream<TrafficPageViewBean> stream) {
        return stream
                // 基于事件时间，加水印
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                                .withTimestampAssigner((bean,ts)->bean.getTs())
                )
                // 分组 渠道_地区_访客类别_版本
                .keyBy(bean->bean.getCh() + "_" + bean.getAr() + "_" + bean.getIsNew() + "_" + bean.getVc())
                // 开窗，滚动时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // uv和uj数据迟到：pv数据是直接从数据源拿来的，uv和uj是要经过去重处理写入uv日志，再从uv/uj日志取，中间经历一个环节
                // 因为 pv先来，开启窗口，uv和uj数据迟到，加不进去，最好的方法是在水印设置中加大乱序时间，或者窗口加大也行
                .sideOutputLateData(new OutputTag<TrafficPageViewBean>("lateData") {})  // 开一个侧输出流，测试是否有迟到数据
                .reduce(new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean bean1,
                                                      TrafficPageViewBean bean2) throws Exception {
                        bean1.setUvCt(bean1.getUvCt() + bean2.getUvCt());
                        bean1.setSvCt(bean1.getSvCt() + bean2.getSvCt());
                        bean1.setPvCt(bean1.getPvCt() + bean2.getPvCt());
                        bean1.setDurSum(bean1.getDurSum() + bean2.getDurSum());
                        bean1.setUjCt(bean1.getUjCt() + bean2.getUjCt());
                        return bean1;
                    }
                }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                        TrafficPageViewBean bean = elements.iterator().next();

                        // 设置窗口的开始时间和结束时间
                        bean.setStt(DateFormatUtil.toYmdHms(context.window().getStart()));
                        bean.setEdt(DateFormatUtil.toYmdHms(context.window().getEnd()));

                        // ts原本是事件时间，但两bean相加的时候，ts实际上还是bean1的时间，故改统计时间：什么时候把这个结果最终输出的，其实不改也行，后期不用了
                        bean.setTs(context.currentProcessingTime());

                        out.collect(bean);

                    }
                });
    }



    /**
     * 读取多个topic并解析，取数据把多个流union成一个流，存在一个自建bean
     * @param streams：多个流
     * @return 返回一个流, 存储自建bean
     */
    private DataStream<TrafficPageViewBean> unionOne(Map<String, DataStreamSource<String>> streams) {
        // 先取 pv表数据  sv(会话数) durSum(累计访问时长),存入创建的TrafficPageViewBean，这个bean里面有pv、uv、uj以及相关属性
        SingleOutputStreamOperator<TrafficPageViewBean> pvSvDurSumStream = streams.get(TOPIC_DWD_TRAFFIC_PAGE)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    JSONObject page = obj.getJSONObject("page");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 因为是从pv表取的一条数据，pv计数1
                    Long pv = 1L;
                    // last_page_id空说明页面是首页，开启了一个新的会话，将会话数置为 1，否则置为 0
                    Long sv = page.getString("last_page_id") == null ? 1L : 0L;
                    Long durSum = page.getLong("during_time");

                    Long ts = obj.getLong("ts");

                    return new TrafficPageViewBean("", "", vc, ch, ar, isNew, 0L, sv, pv, durSum, 0L, ts);
                });

        // 取 uv表数据
        SingleOutputStreamOperator<TrafficPageViewBean> uvStream = streams.get(TOPIC_DWD_TRAFFIC_UV)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    Long uv = 1L;

                    Long ts = obj.getLong("ts");

                    return new TrafficPageViewBean("", "", vc, ch, ar, isNew, uv, 0L, 0L, 0L, 0L, ts);
                });

        // 取 uj表数据
        SingleOutputStreamOperator<TrafficPageViewBean> ujStream = streams.get(TOPIC_DWD_TRAFFIC_UJ_DETAIL)
                .map(json -> {
                    JSONObject obj = JSON.parseObject(json);
                    JSONObject common = obj.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    Long uj = 1L;

                    Long ts = obj.getLong("ts");

                    return new TrafficPageViewBean("", "", vc, ch, ar, isNew, 0L, 0L, 0L, 0L, uj, ts);
                });

        return pvSvDurSumStream.union(uvStream, ujStream);

    }
}
