# 一个基于flink框架的实时数仓小项目

数仓模型各层的技术选择

ODS

    存储在Kafka中

        ods_log
            所有的日志数据
                前端埋点采集数据，原始数据通过Nginx存于日志服务器
                日志服务器内的logFile通过Flume采集到KafKa

        ods_db
            所有的业务数据（维度表数据也在）
                原始数据通过Nginx存于业务服务器内的MySQL数据库中
                通过Maxwell实时同步到Kafka


DWD
    
    存储到Kafka中

        处理好的明细数据
            日志数据：对ods_log进行分流，不同日志写入到不同的topic（Flink侧输出流实现分流）
                本项目有5个topic：启动 页面 曝光 错误 行动

            业务数据：不选择分流处理，直接用到什么明细就从ods_db中消费，通过Flink过滤出来需要的明细
                维度退化（维度冗余）：做明细的时候直接把一些维度表数据冗余进去，主要是base_dic数据


DIM

    维度层，来源：ods_db

    存储到HBase中
        维度怎么用--->根据用户id去查找对应的维度信息--->select...from T where id=？，那么就有以下工具选择
            mysql：项目频繁查询mysql，极有可能影响业务表写入
            hive：查询速度慢，无法接入在线分析
            hbase+phoenix：可以，数据容量满足，支持sql，选这个技术
            elasticSearch：可以，定点查询大材小用，一般拿来搜索
            ......


DWS

    汇总层：聚合
    轻度聚合：小窗口聚合
    10秒一个窗口，0~10、10~20这样子去做聚合，将来想要哪个时间段就将数据过滤出来做二次聚合


    来源DWD+DIM

    存储到：关系型数据库，选clickhouse
        hbase+phoenix：可以选，但反正不是生产环境为啥不练点别的，老是一个工具多没意思
        clickhouse：关系型的OLAP数据库，可以选，选这个
        Doris：新东西，听说很牛b，但是刚出bug多，等什么时候成熟了直接连Kafka负责的部分也可以换掉


ADS
    
    不落盘，将来会直接执行聚合结果，给前端展示
    sql语句做聚合