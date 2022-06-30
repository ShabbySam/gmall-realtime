package com.myPractice.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.bean.TradeSkuOrderBean;
import com.myPractice.realtime.common.Constant;
import com.myPractice.realtime.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.time.Duration;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/28 10:06
 *
 *
 * 交易域用户-SPU粒度下单各窗口汇总表
 *  来源：订单详情表
 *      详情表来源：预处理表
 *          预处理表中有左连接 -> 有重复数据 -> 去重
 *  去重
 *      有一条详情数据出现大量重复
 *          粒度：sku
 *              detail_1  sku_1   100    null（右表还没来，就先null）
 *              detail_1  sku_1   100     10 （右表来了）
 *          去重的时候按照detail_1去重，一个sku可能有多个用户购买，所以不能用sku去重
 *              保留最后一条数据 -> 时间戳最大
 *
 *              1. 如何找到最后一条：
 *                  详情表 left join 详情活动 left join 详情优惠券
 *                  5s内，如果有的话一定来齐（具体时间看各自网络）
 *                      定时器：
 *                          第一条来的时候注册定时器
 *                              中间每来一条数据就判断时间，如果来的时间大于状态中存储的那个时间，更新
 *                          等定时器触发，最后一条一定来了
 *
 *              2. 去重简化：
 *                  为什么一定要用最后一条？因为最后一条的数据比较完整
 *                      如果在统计计算的时候根本用不到右表数据，这种情况只要第一条就行
 *
 *              3. 用窗口 session
 *                  gap 5秒
 *                  当窗口关闭的时候，同一个详情的数据一定都到了，再从这个窗口内找到操作时间最大那个
 *                      时效性较低
 *
 *
 *  按照ku分组，开窗，聚合
 *
 *  补充维度信息：详情中只有一个sku_id
 *      sku_name
 *       spu
 *       tm
 *       ...
 *
 *  最后写入到clickhouse中
 *
 */
public class Dws_09_DwsTradeSkuOrderWindow_Cache extends BaseAppV1 {
    public static void main(String[] args) {
        new Dws_09_DwsTradeSkuOrderWindow_Cache().init(
                4396,
                2,
                "dws_09_dwsTradeSkuOrderWindow",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 先找order_detail_id进行去重
        SingleOutputStreamOperator<JSONObject> distinctedStream = distinctByOrderDetailId(stream);

        // 2. 根据需要去除一些字段，封装到bean
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(distinctedStream);
        //beanStream.print();

        // 3. 按照sku_id进行分组开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> streamWithoutDim = windowAndAggregate(beanStream);
        //streamWithoutDim.print();

        // 4. 补充维度信息
        joinDim(streamWithoutDim);


        // 5. 写出到clickhouse中
    }

    /**
     * 添加维度信息 -> 读取储存在hbase里的维度表
     *
     * 频繁的数据库读取影响性能（数据库性能/网络性能），也会导致业务数据无法写入数据库
     *     加缓冲：把读取到的未读数据存入到内存中，下次使用同一条维度数据的时候，先从内存读取，如果没有，再从数据库读取
     *         flink状态：
     *          好处：
     *              本地内存，不走网络，读写性能极快
     *
     *          坏处：
     *              1. 占用flink的内存，影响flink的计算，如果数据量大，会Boom，有钱加内存另说
     *              2. 数据一致性问题：当维度发生变化，状态中的值没办法及时更新
     *
     *         redis：旁路缓存
     *          好处：
     *              数据一致性：一旦维度发生变化，redis中的值就会及时更新
     *                  DimApp在通过读取db更新信息对hbase内的表进行更新的时候，也可以同时对redis进行更新
     *
     *          坏处：
     *              每次读取redis都需要通过网络
     *
     *
     * redis数据结构的选择 -> string
     *  string
     *    key：表名:id  value：维度信息的json串
     *
     *      好处：
     *          1. 方便读写
     *          2. 可以单独给每个维度设置过期时间ttl
     *      坏处：
     *          redis中key的个数非常多，不太方便管理，而且有与其他的key冲突的可能性
     *          解决办法：把维度数据放进专门的库
     *
     *  list
     *    key：表名    value：维度信息的json串
     *
     *      好处：
     *          读方便
     *      坏处：
     *          需要读取这张表的所有缓存数据，然后遍历
     *          ttl一张表内的数据会同时失效
     *
     *
     *  set
     *      相对list就是多了个去重，没有去重需求
     *
     *
     *  hash
     *    key：表名    field：id    value：维度信息的json串
     *
     *      好处：只有6个key
     *      坏处：ttl一张表内的数据会同时失效
     *
     *
     *  zset
     *   没有排序需求，没必要
     *
     */
    private void joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        stream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

            private Jedis redisClient;
            private Connection phoenixConn;

            /**
             * 获取phoenix连接、redis连接
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                phoenixConn = JdbcUtil.getPhoenixConnection();
                redisClient = RedisUtil.getRedisClient();
            }

            /**
             * 关闭phoenix连接、redis连接
             * @throws Exception
             */
            @Override
            public void close() throws Exception {
                if (phoenixConn != null) {
                    phoenixConn.close();
                }
                if (redisClient != null) {
                    redisClient.close();
                }
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean value) throws Exception {
                // 1. 根据sku_id查询sku表：c3_id, spu_id, tm_id
                // 查询sku_info 所有的维度信息    key:字段名  value:值
                JSONObject skuInfo = DimUtil.readDim(redisClient, phoenixConn, "dim_sku_info", value.getSkuId());
                // {"SPU_ID": "1", "SKU_NAME": "abc"}
                System.out.println("skuInfo: " + skuInfo);

                value.setSkuName(skuInfo.getString("SKU_NAME"));
                value.setSpuId(skuInfo.getString("SPU_ID"));
                value.setTrademarkId(skuInfo.getString("TM_ID"));
                value.setCategory3Id(skuInfo.getString("CATEGORY3_ID"));

                // 2. base_trademark
                JSONObject baseTrademark = DimUtil.readDim(redisClient, phoenixConn, "dim_base_trademark", value.getTrademarkId());
                value.setTrademarkName(baseTrademark.getString("TM_NAME"));

                // 3. spu
                JSONObject spuInfo = DimUtil.readDim(redisClient, phoenixConn, "dim_spu_info", value.getSpuId());
                value.setSpuName(spuInfo.getString("SPU_NAME"));

                // 4. c3
                JSONObject c3 = DimUtil.readDim(redisClient, phoenixConn, "dim_base_category3", value.getCategory3Id());
                value.setCategory3Name(c3.getString("NAME"));
                value.setCategory2Id(c3.getString("CATEGORY2_ID"));

                // 5. c2
                JSONObject c2 = DimUtil.readDim(redisClient, phoenixConn, "dim_base_category2", value.getCategory2Id());
                value.setCategory2Name(c2.getString("NAME"));
                value.setCategory1Id(c2.getString("CATEGORY1_ID"));

                // 6. c1
                JSONObject c1 = DimUtil.readDim(redisClient, phoenixConn, "dim_base_category1", value.getCategory1Id());
                value.setCategory1Name(c1.getString("NAME"));
                return value;
            }
        }).print();
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAggregate(
       SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        return stream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                )
                .keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1,
                                                            TradeSkuOrderBean value2) throws Exception {
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                                value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                                value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());

                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<TradeSkuOrderBean> elements,
                                                Collector<TradeSkuOrderBean> out) throws Exception {

                                TradeSkuOrderBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.toYmdHms(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.toYmdHms(ctx.window().getEnd()));

                                bean.setTs(System.currentTimeMillis());

                                // 根据set集合的长度去设置orderCount的值
                                bean.setOrderCount((long) bean.getOrderIdSet().size());

                                out.collect(bean);


                            }
                        }
                );
    }


    /**
     * 根据需要去除一些字段，封装到bean
     * @param stream 去重后的流
     * @return
     */
    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(SingleOutputStreamOperator<JSONObject> stream) {
        return stream
                .map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject value) throws Exception {
                        TradeSkuOrderBean bean = TradeSkuOrderBean.builder()
                                .skuId(value.getString("sku_id"))

                                // getDoubleValue(): 如果字段的值是null, 则会赋值0D
                                .originalAmount(value.getDoubleValue("split_original_amount"))
                                .activityAmount(value.getDoubleValue("split_activity_amount"))
                                .couponAmount(value.getDoubleValue("split_coupon_amount"))
                                .orderAmount(value.getDoubleValue("split_total_amount"))
                                .ts(value.getLong("ts") * 1000)
                                .build();

                        bean.getOrderIdSet().add(value.getString("order_id"));

                        return bean;
                    }
                });
    }


    /**
     * 先找order_detail_id进行去重
     * @param stream 从dwd_trade_order_detail获取的数据源
     */
    private SingleOutputStreamOperator<JSONObject> distinctByOrderDetailId(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> maxDateDataState;

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        // 定时器触发的时候，状态中保存的一定是时间最大的那条数据，最后一个最完整的数据
                        out.collect(maxDateDataState.value());
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        maxDateDataState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("maxDateDataState", JSONObject.class));
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        if (maxDateDataState.value() == null) {
                            // null，说明第一条数据进来
                            // 1. 注册定时器，5s后触发
                            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
                            // 2. 更新状态
                            maxDateDataState.update(value);
                        } else {
                            // 不是第一条
                            // 3. 比较时间，新来的数据如果时间比较大就保存它的数据（并更新状态），注意，时间后面加了个z，写个方法去掉
                            String current = value.getString("row_op_ts");
                            String last = maxDateDataState.value().getString("row_op_ts");
                            if (MyUtil.compareLTZ(current, last)) {
                                maxDateDataState.update(value);
                            }
                        }
                    }
                });
    }
}
