package com.myPractice.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.sink.FlinkSinkUtil;
import com.myPractice.realtime.util.DateFormatUtil;
import com.myPractice.realtime.util.MyUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/17 8:52
 *
 * 流量域未经加工的事实表分析
 * 主要任务
 * 1. 数据清洗
 * 2. 纠正新老客户 标记
 * 3. 分流
 *   通过分流对日志数据进行拆分，生成五张事务事实表写入 Kafka
 *
 *   其中，每张事务事实表的类别是：
 *   	流量域页面浏览事务事实表 page
 *   	流量域启动事务事实表 start
 *   	流量域动作事务事实表 action
 *   	流量域曝光事务事实表 display
 *   	流量域错误事务事实表 error
 */
public class Dwd_01_BaseLogApp extends BaseAppV1 {

    private final String ERROR = "err";
    private final String PAGE = "page";
    private final String ACTION = "action";
    private final String DISPLAY = "display";
    private final String START = "start";

    public static void main(String[] args) {
        // 创建flink流处理的环境
        new Dwd_01_BaseLogApp().init(10000, 2, "Dwd_01_BaseLogApp", Constant.TOPIC_ODS_LOG);
    }

    /**
     * 流处理的主逻辑
     * @param env: 执行流程序的环境
     * @param stream：创建的流
     */
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 数据清洗
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);

        // 2. 纠正新老客户标记
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(etledStream);

        // 3. 分流
        Map<String, DataStream<String>> streams = splitStream(validatedStream);


        // 4. 写入kafka
        writeToKafka(streams);
    }

    /**
     * 不同的事务事实表分别写入不同的topic
     * @param streams：key为事务事实表类别，value为事务事实表数据
     */
    private void writeToKafka(Map<String, DataStream<String>> streams) {
        streams.get(PAGE).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        streams.get(ERROR).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERROR));
        streams.get(DISPLAY).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        streams.get(ACTION).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        streams.get(START).addSink(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
    }


    /**
     * 数据分流
     *  主流：启动
     *  副流：错误 页面 曝光 行动
     * @param stream ：经过纠正的数据流
     * @return
     */
    private Map<String, DataStream<String>> splitStream(SingleOutputStreamOperator<JSONObject> stream) {
        // 创建副流
        OutputTag<String> errTag = new OutputTag<String>(ERROR) {};
        OutputTag<String> pageTag = new OutputTag<String>(PAGE) {};
        OutputTag<String> actionTag = new OutputTag<String>(ACTION) {};
        OutputTag<String> displayTag = new OutputTag<String>(START) {};

        // 只有process function才能获取到outputTag侧输出流
        SingleOutputStreamOperator<String> startStream = stream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                // 所有的页面都可能有错误，所以先判断错误
                if (value.containsKey("err")) {
                    ctx.output(errTag, value.toJSONString());
                    // 错误信息对其他日志没什么用，删除
                    value.remove("err");
                }

                // 判断是否是启动日志，除去错误后启动行为一定是跟其他行为互斥的，而页面、曝光、行动这个行为是可以重合的
                if (value.containsKey("start")) {
                    // 启动日志存入主流
                    out.collect(value.toJSONString());
                } else {
                    // 其他日志,存入副流的时候连同其他信息一同存入，尽量信息完整
                    JSONObject common = value.getJSONObject("common");
                    JSONObject page = value.getJSONObject("page");
                    Long ts = value.getLong("ts");

                    // 判断是否是曝光日志，曝光日志的内容一般由多个页面组成，所以需要遍历页面，压平
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        // 曝光日志遍历
                        for (int i = 0; i < displays.size(); i++) {
                            // 创建一个新的，用于存储曝光日志的信息
                            JSONObject display = displays.getJSONObject(i);
                            // 往JSONObject放入其他日志信息
                            display.putAll(common);
                            display.putAll(page);
                            display.put("ts", ts);
                            // 存入副流
                            ctx.output(displayTag, display.toJSONString());
                        }
                        // 删除曝光日志
                        value.remove("displays");
                    }

                    // 判断是否是行动日志，行动日志的内容一般由多个页面组成，所以需要遍历页面
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        // 行动日志遍历
                        for (int i = 0; i < actions.size(); i++) {
                            // 创建一个新的，用于存储行动日志的信息
                            JSONObject action = actions.getJSONObject(i);
                            // 往JSONObject放入其他日志信息
                            action.putAll(common);
                            action.putAll(page);
                            action.put("ts", ts);
                            // 存入副流
                            ctx.output(actionTag, action.toJSONString());
                        }
                        // 删除行动日志
                        value.remove("actions");
                    }

                    // 页面日志，除了启动日志，其他日志都是页面日志，所以直接存入副流
                    if (value.containsKey("page")) {
                        ctx.output(pageTag, value.toJSONString());
                    }
                }
            }
        });
        /**
         * 返回多个流
         *  1. list集合  取的顺序要和存的顺序一致
         *  2. 元组  少的还好，多了也不好，也有顺序
         *  3. map集合 给定key，取出对应的value
         */
        Map<String, DataStream<String>> result = new HashMap<>();
        result.put(ERROR, startStream.getSideOutput(errTag));
        result.put(PAGE, startStream.getSideOutput(pageTag));
        result.put(ACTION, startStream.getSideOutput(actionTag));
        result.put(DISPLAY, startStream.getSideOutput(displayTag));
        result.put(START, startStream);

        return result;
    }






    /**
     * 修正新老客户标记
     * 什么是新老客户？什么样的用户会标记错误？
     *        如果来的是老客户，肯定不会标记错误，如果是新客户，那么就有可能是错的，比如app卸载重装
     *
     *        设置状态存储：用户第一次的访问日期：年月日；用于判断是否是新客户
     *
     *        is_new是新用户标记字段
     *            is_new = 0，表示老客户，不用操作
     *
     *            is_new = 1
     *                状态是null
     *                    把今天的日期存储到状态中
     *
     *                状态不是null
     *                    今天和状态中的日期比较
     *                        一样，不用操作
     *                        不一样，说明不是新用户，把is_new置为0
     */
    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> stream) {

        return stream
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))

                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> firstDateState;

                    /**
                     * 在初始化方法创建状态
                     * @param parameters
                     * @throws Exception
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_date", String.class));
                    }

                    /**
                     * 每条数据的处理逻辑
                     * @param value：当前的数据
                     * @param ctx：上下文
                     * @param out：输出流
                     * @throws Exception
                     */
                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 提取json中的is_new字段和时间戳
                        JSONObject common = value.getJSONObject("common");
                        String isNew = common.getString("is_new");
                        Long ts = value.getLong("ts");

                        // 取出状态中的日期
                        String firstDate = firstDateState.value();
                        // ts转化为日期
                        String date = MyUtil.toDate(ts);

                        // 如果is_new是1，那么就需要判断状态是否为null
                        if ("1".equals(isNew)) {
                            // 如果状态为null，说明是第一条访问记录，那么就把今天的日期存储到状态中
                            if (firstDate == null) {
                                firstDateState.update(date);
                            } else {
                                // 如果状态不为null，那么就需要判断今天的日期和状态中的日期是否一样
                                if (!date.equals(firstDate)) {
                                    // 如果时间不一样，说明不是新用户，把is_new置为0
                                    common.put("is_new", "0");
                                }
                            }
                        } else if (firstDate == null) {
                            // 老用户，状态应该有值
                            // 如果没有值，给状态添加一个以前的日期（昨天），用于以后纠正
                            firstDateState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000));
                        }
                        out.collect(value);
                    }
                });
    }


    /**
     * 数据清洗,剔除不是json格式的数据,并且把String转换为jsonObject
     * @param stream：输入流
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.filter(json -> {
            try {
                JSON.parseObject(json);
                return true;
            } catch (Exception e) {
                System.out.println("不是json");
                return false;
            }
        })
                .map(JSON::parseObject);
    }
}
