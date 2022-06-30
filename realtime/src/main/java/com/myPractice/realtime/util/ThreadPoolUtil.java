package com.myPractice.realtime.util;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/29 20:50
 *
 * 线程池
 */
public class ThreadPoolUtil {
    public static Executor getThreadPool() {
        return new ThreadPoolExecutor(
                300,
                400,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100)
        );
    }
}
