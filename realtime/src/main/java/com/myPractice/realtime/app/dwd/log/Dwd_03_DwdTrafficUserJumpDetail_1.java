package com.myPractice.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/19 16:21
 *
 *
 * 跳出率
 *
 * 跳出 指访问的入口页面，然后就关闭页面，没有继续访问其他页面
 * 跳出率 跳出次数/进入（session）的次数
 *
 * ——————————————————
 * 不可加
 * 进入次数：
 *     进入的记录 计数
 *
 *     如果找到进入页面：last_page_id == null
 *
 * 跳出次数
 *     找到跳出记录 计数
 *
 *     怎么找到跳出记录？
 *         找到跳出明细
 *
 *         从流中找出符合我们需要的数据，那些只有last_page_id == null后面没跟着别的记录的
 *
 *             使用cep
 *
 *
 *     先找一个入口，这个入口后没有跟着正常页面
 */
public class Dwd_03_DwdTrafficUserJumpDetail_1 extends BaseAppV1 {
    public static void main(String[] args) {
        new Dwd_03_DwdTrafficUserJumpDetail_1().init(9998,2,"Dwd_03_DwdTrafficUserJumpDetail_1", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {


        /*
//        测试代码
        stream = env
                .fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":11000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":18000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":30000} "
                );*/


        KeyedStream<JSONObject, String> keyedStream = stream
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts"))
                )
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"));

        // 1. 先定义模板
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("entry1")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.isEmpty();
                    }
                })
                .next("entry2")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.isEmpty();
                    }
                })
                .within(Time.seconds(3));

        // 2. 把模式作用到流上，得到一个模式流
        PatternStream<JSONObject> ps = CEP.pattern(keyedStream, pattern);

        // 3. 从模式流中匹配到的数据或者超时的数据
        SingleOutputStreamOperator<JSONObject> normalStream = ps.select(
                new OutputTag<JSONObject>("late") {
                },
                // 这里是超时数据，也属于
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> pattern, long l) throws Exception {
                        return pattern.get("entry1").get(0);
                    }
                },
                // 这里是符合定义模板的情况
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return pattern.get("entry1").get(0);
                    }
                }
        );

        normalStream.getSideOutput(new OutputTag<JSONObject>("late"){}).union(normalStream)
                .map(JSONAware::toJSONString)
                .addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_UJ_DETAIL));
    }
}
