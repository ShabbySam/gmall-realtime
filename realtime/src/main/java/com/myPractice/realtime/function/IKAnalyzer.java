package com.myPractice.realtime.function;

import com.myPractice.realtime.util.IkUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/24 18:42
 */
@FunctionHint(output = @DataTypeHint("row<kw string>"))  // 用注解指定函数的输出类型row的列名和类型
public class IKAnalyzer extends TableFunction<Row> {

    public void eval(String keyword){
        //  把keyword进行分词 小米手机 -> (小米,手机)
        List<String> kws = IkUtil.split(keyword);
        // list有多少个字符串，就有多少行
        for (String kw : kws) {
            collect(Row.of(kw));
        }
    }
}
