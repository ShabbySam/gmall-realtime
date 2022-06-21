import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lzc
 * @Date 2022/6/18 11:22
 */
public class Join2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
    
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));
        tEnv.getConfig().getConfiguration().setString("table.exec.state.ttl", "10 s");
        
        tEnv.executeSql("create table s1(id string, name string)with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            " 'properties.group.id' = 'atguigu', " +
                            " 'topic' = 's1'," +
                            " 'format'='csv' " +
                            ")");
    
        tEnv.executeSql("create table s2(id string, age int)with(" +
                            " 'connector'='kafka', " +
                            " 'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            " 'properties.group.id' = 'atguigu', " +
                            " 'topic' = 's2', " +
                            " 'format'='csv' " +
                            ")");
        
        
        // 左连接，左连接的左表每使用一次，表存活时间会重新计算，所以当右表一直有数据传进来，左表的存活时间就会变长
        Table result = tEnv.sqlQuery("select " +
                                        " s1.id, " +
                                        " name, " +
                                        " age " +
                                        "from s1 " +
                                        "left join s2 on s1.id=s2.id");

        // 写入kafka必须设置主键，通过主键来区别是更新还是新增
        tEnv.executeSql("create table s12(id string, name string, age int, primary key(id) not enforced)" +
                            "with(" +
                            " 'connector'='upsert-kafka', " +
                            " 'properties.bootstrap.servers' = 'hadoop162:9092', " +
                            " 'topic' = 's12'," +
                            " 'key.format'='json', " +
                            " 'value.format'='json' " +
                            ")");

        // kafka的数据更新是先写一个null表示删除，再把新数据写入
        result.executeInsert("s12");
        
    
    }
        
    
    
        
    
}
