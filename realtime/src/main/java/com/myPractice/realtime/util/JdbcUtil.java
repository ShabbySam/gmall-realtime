package com.myPractice.realtime.util;

import com.myPractice.realtime.common.Constant;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/15 19:36
 */
public class JdbcUtil {
    public static Connection getPhoenixConnection() {
        String driver = Constant.PHOENIX_DRIVER;
        String url = Constant.PHOENIX_URL;
        Connection conn = null;
        try {
            conn = getJdbcConnection(driver, url, null, null);
        } catch (ClassNotFoundException e) {
            // 驱动类没有找到
            throw new RuntimeException("phoenix jdbc driver not found, please check your classpath");
        } catch (SQLException e) {
            // sql异常
            throw new RuntimeException("get phoenix connection error, please check your zookeeper url");
        }
        return conn;
    }

    private static Connection getJdbcConnection(String driver, String url, String user, String password) throws ClassNotFoundException, SQLException {
            // 加载驱动
            Class.forName(driver);
            return DriverManager.getConnection(url, user, password);
    }
}
