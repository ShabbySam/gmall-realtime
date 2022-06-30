package com.myPractice.realtime.bean;

import com.myPractice.realtime.annotation.NoSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.Set;

@Data
@AllArgsConstructor
@Builder
public class TradeProvinceOrderWindow {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 省份 ID
    String provinceId;

    // 省份名称
    @Builder.Default
    String provinceName = "";

    // 累计下单次数
    Long orderCount;

    // 订单 ID 集合，用于统计下单次数
    @NoSink
    Set<String> orderIdSet;

    // 累计下单金额
    Double orderAmount;

    // 时间戳
    Long ts;
}
