#!/bin/bash

flink=/opt/module/flink-1.13.6/bin/flink
jar=/opt/gmall211227/realtime-1.0-SNAPSHOT.jar
apps=(
com.myPractice.realtime.app.dim.DimApp
com.myPractice.realtime.app.dwd.log.Dwd_01_BaseLogApp
com.myPractice.realtime.app.dwd.log.Dwd_02_DwdTrafficUniqueVisitorDetail
com.myPractice.realtime.app.dwd.log.Dwd_03_DwdTrafficUserJumpDetail_1
com.myPractice.realtime.app.dwd.db.Dwd_04_DwdTradeCartAdd
com.myPractice.realtime.app.dwd.db.Dwd_05_DwdTradeOrderPreProcess
com.myPractice.realtime.app.dwd.db.Dwd_06_DwdTradeOrderDetail
com.myPractice.realtime.app.dwd.db.Dwd_07_DwdTradeCancelDetail
com.myPractice.realtime.app.dwd.db.Dwd_08_DwdTradePayDetailSuc
com.myPractice.realtime.app.dwd.db.Dwd_09_DwdTradeOrderRefund
com.myPractice.realtime.app.dwd.db.Dwd_10_DwdTradeRefundPaySuc
com.myPractice.realtime.app.dwd.db.Dwd_11_DwdToolCouponGet
com.myPractice.realtime.app.dwd.db.Dwd_12_DwdToolCouponOrder
com.myPractice.realtime.app.dwd.db.Dwd_13_DwdToolCouponPay
com.myPractice.realtime.app.dwd.db.Dwd_14_DwdInteractionFavorAdd
com.myPractice.realtime.app.dwd.db.Dwd_15_DwdInteractionComment
com.myPractice.realtime.app.dwd.db.Dwd_16_DwdUserRegister
)

running_apps=`$flink list 2>/dev/null | awk  '/RUNNING/ {print \$(NF-1)}'`

for app in ${apps[*]} ; do
    app_name=`echo $app | awk -F. '{print \$NF}'`

    if [[ "${running_apps[@]}" =~ "$app_name" ]]; then
        echo "$app_name 已经启动,无序重复启动...."
    else
         echo "启动应用: $app_name"
        $flink run -d -c $app $jar
    fi
done




