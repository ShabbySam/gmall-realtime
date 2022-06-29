package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.UserLoginBean;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/27 2:49
 *
 *
 * 用户域用户登陆各窗口汇总表
 *  需求：七日回流用户，当日独立用户数
 *      当日登陆，且自上次登陆之后至少 7 日未登录的用户为回流用户
 *  from：页面日志 dwd_traffic_page
 */
public class Dws_04_DwsUserUserLoginWindow extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_04_DwsUserUserLoginWindow().init(
                4396,
                2,
                "Dws_04_DwsUserUserLoginWindow",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
                );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 过滤出登录记录
        SingleOutputStreamOperator<JSONObject> loginStream = filterLoginData(stream);

        // 2. 找到今天首次登录和今天7回流首次登录
        SingleOutputStreamOperator<UserLoginBean> beanStream = findUvAndBack(loginStream);


        // 3. 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultStream = windowAndAggregate(beanStream);


        // 4. 写出到clickhouse中
        writeToClickhouse(resultStream);

    }

    /**
     * 写出到clickhouse中
     * @param resultStream
     */
    private void writeToClickhouse(SingleOutputStreamOperator<UserLoginBean> resultStream) {
        resultStream.addSink(FlinkSinkUtil.getClickHouseSink("dws_user_user_login_window", UserLoginBean.class));
    }

    /**
     * 开窗聚合
     * @param beanStream
     * @return
     */
    private SingleOutputStreamOperator<UserLoginBean> windowAndAggregate(SingleOutputStreamOperator<UserLoginBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())

                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                            @Override
                            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<UserLoginBean> elements, Collector<UserLoginBean> out) throws Exception {
                                UserLoginBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.toYmdHms(context.window().getStart()));
                                bean.setEdt(DateFormatUtil.toYmdHms(context.window().getEnd()));

                                bean.setTs(System.currentTimeMillis());

                                out.collect(bean);
                            }
                        });
    }

    /**
     * 找到今天首次登录和今天7回流首次登录
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<UserLoginBean> findUvAndBack(SingleOutputStreamOperator<JSONObject> stream) {
        return stream
                .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 记录该用户最后一次登录的年月日
                        lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDateState", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj, Context ctx, Collector<UserLoginBean> out) throws Exception {
                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.toDate(ts);
                        String lastLoginDate = lastLoginDateState.value();

                        long uuCt = 0;
                        long backCt = 0;

                        if (!today.equals(lastLoginDate)){
                            // 数据日期与状态日期不同，今日首次登录
                            System.out.println("uid: " + ctx.getCurrentKey()); // 上面已经keyBy，uid就是key
                            uuCt = 1;

                            if (lastLoginDate != null){ // 有曾经登陆过的状态，才有可能是回流用户
                                Long lastLoginTs = DateFormatUtil.toTs(lastLoginDate);
                                // 判断是否为7日回流 （今天数据的ts - 状态存储的ts）> 7天
                                if ((ts - lastLoginTs) / 1000 / 60 / 60 / 24 > 7 ){
                                    backCt = 1;
                                }
                            }
                        }
                        // 有登录的数据才要
                        if (uuCt == 1){
                            out.collect(new UserLoginBean("", "", backCt, uuCt, ts));
                        }
                    }
                });
    }



    /**
     * 过滤出登录记录
     *  1. uid不为空
     *  2. 登陆分为两种情况
     *      1）用户打开应用后自动登录 -> 登录操作发生在会话首页，所以保留首页 -> uid != null && lastPageId == null
     *      2）用户打开应用后没有登陆，浏览部分页面后跳转到登录页面，中途登陆 -> 登录操作发生在登录页面，所以保留登录页面 -> uid != null && lastPageId == 'login'
     * @param stream
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> filterLoginData(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .filter(obj -> {
                    String uid = obj.getJSONObject("common").getString("uid");
                    JSONObject page = obj.getJSONObject("page");
                    String pageId = page.getString("page_id");
                    String lastPageId = page.getString("last_page_id");

                    return uid != null && (lastPageId == null || "login".equals(lastPageId));
                });
    }
}
