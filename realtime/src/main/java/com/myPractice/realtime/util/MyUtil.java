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

    public static boolean compareLTZ(String a, String b) {
        String aNoZ = a.replace("Z", "");
        String bNoZ = b.replace("Z", "");

        // x.compareTo(y)实际上是有一个int返回值的，当x<y时，返回-1，当x>y时，返回1，当x=y时，返回0。
        return aNoZ.compareTo(bNoZ) >= 0; // 如果aNoZ>=bNoZ，返回true，否则返回false。
    }

    public static void main(String[] args) {
        System.out.println("2022-06-27 01:04:48.839".compareTo("2022-06-28 01:04:48.822"));
        System.out.println(compareLTZ("2022-06-27 01:04:48.839Z", "2022-06-28 01:04:48.822Z"));
    }
}
