import com.myPractice.realtime.app.BaseAppV1;
import com.myPractice.realtime.common.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/20 15:50
 */
public class join12 extends BaseAppV1 {
    public static void main(String[] args) {
        new join12().init(
            2003,
            2,
            "join12",
                "s12"
        );
    }
    // 用流去消费存入kafka里的s12，
    // 因为数据更新产生了null，
    // 所以原本FlinkSourceUtil的FlinkKafkaConsumer方法引用的SimpleStringSchema()会报错空指针，
    // 所以要去自定义反序列化器
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
