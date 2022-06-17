package com.myPractice.realtime.util;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author lzc
 * @Date 2022/6/17 9:22
 */
public class MyUtil {
    public static String toDate(Long ts) {
        return new SimpleDateFormat("yyyy-MM-dd").format(ts);
    }

    public static <T> List<T> toList(Iterable<T> it) {
        ArrayList<T> result = new ArrayList<>();
        for (T t : it) {
            result.add(t);
        }
        return result;
    }
}
