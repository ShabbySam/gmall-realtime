package com.myPractice.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/24 18:48
 */
public class IkUtil {
    // 使用ik分词器把传入的字符进行分词
    public static List<String> split(String keyword) {
        ArrayList<String> result = new ArrayList<>();

        // IKSegmenter的参数需要是流 将字符串转换成流
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        Lexeme next = null;
        try {
            next = ikSegmenter.next();
            while (next != null) {
                String kw = next.getLexemeText();
                result.add(kw);

                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // list集合可能有重复元素，搞个去重
        HashSet<String> set = new HashSet<>(result);
        result.clear();
        result.addAll(set);

        return result;
    }

    public static void main(String[] args) {
        System.out.println(split("小米手机"));
    }
}
