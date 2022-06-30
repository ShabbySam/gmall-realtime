package com.myPractice.realtime.bean;

import com.myPractice.realtime.annotation.NoSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashSet;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeSkuOrderBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // 二级品类 ID
    String category2Id;
    // 二级品类名称
    String category2Name;
    // 三级品类 ID
    String category3Id;
    // 三级品类名称
    String category3Name;
    // sku_id
    String skuId;
    // sku 名称
    String skuName;
    // spu_id
    String spuId;
    // spu 名称
    String spuName;

    //-----
    // 订单 ID
    @NoSink
    @Builder.Default
    Set<String> orderIdSet = new HashSet<>();
    
    // 下单次数
    @Builder.Default
    Long orderCount = 0L;
    // 原始金额
    @Builder.Default
    Double originalAmount = 0D;
    // 活动减免金额
    @Builder.Default
    Double activityAmount = 0D;
    // 优惠券减免金额
    @Builder.Default
    Double couponAmount = 0D;
    // 下单金额
    @Builder.Default
    Double orderAmount = 0D;
    // 时间戳
    Long ts;
    
    
    public static void main(String[] args) {
        TradeSkuOrderBean bean = TradeSkuOrderBean.builder()
            .orderAmount(100D)
            .build();
        System.out.println(bean);
    }
}
