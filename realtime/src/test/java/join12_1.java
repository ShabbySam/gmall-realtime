
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/20 16:07
 */
public class join12_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("create table s12(id string, name string, age int, primary key(id) not enforced)" +
                "with(" +
                " 'connector'='kafka', " +
                " 'properties.bootstrap.servers' = 'hadoop162:9092'," +
                " 'properties.group.id' = 'atguigu', " +
                " 'topic' = 's12'," +
                " 'format'='json'," +
                ")");
        // 用sql消费的时候不用担心null的问题
        tEnv.sqlQuery("select * from s12").execute().print();





    }
}
