import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Author lzc
 * @Date 2022/6/18 11:22
 */
public class Join1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        // 创建表环境
        StreamTableEnvironment tEvn = StreamTableEnvironment.create(env);

        // 限制表状态的存活时间10秒,不加时间久了会爆内存
        /*
        tEvn.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        */
        // 第二种写法
        tEvn.getConfig().getConfiguration().setString("table.exec.state.ttl", "10 s");

        // 创建表s1
        tEvn.executeSql("create table s1(id String, name String) with(" +
                " 'connector'='kafka'," +
                " 'properties.bootstrap.servers'='hadoop162:9092'," +
                " 'properties.group.id'='atguigu'," +
                " 'topic'='s1'," +
                " 'format'='csv'" +
                ")");

        // 创建表s2
        tEvn.executeSql("create table s2(id String, age Int) with(" +
                " 'connector'='kafka'," +
                " 'properties.bootstrap.servers'='hadoop162:9092'," +
                " 'properties.group.id'='atguigu'," +
                " 'topic'='s2'," +
                " 'format'='csv'" +
                ")");

        // 内连接
        tEvn.sqlQuery("select " +
                "s1.id," +
                "name," +
                "age " +
                "from s1 " +
                "join s2 on s1.id=s2.id")
                .execute()
                .print();


    }
        
    
    
        
    
}
