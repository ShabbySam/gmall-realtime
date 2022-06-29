package com.myPractice.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.myPractice.realtime.bean.TableProcess;
import com.myPractice.realtime.common.Constant;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.CaseFormat;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * 使用传来的连接对象，执行sql语句，把查询到的结果封装到list集合中
     * @param conn 传入的连接对象
     * @param sql sql语句
     * @param args  sql语句的参数
     * @param tClass 封装的结果的类型
     * @param underlineToCaseCamel 是否需要将表名的下划线转换为驼峰命名
     * @return
     */
    public static <T>List<T> queryList(Connection conn, String sql, Object[] args, Class<T> tClass, Boolean... underlineToCaseCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        ArrayList<T> result = new ArrayList<>();

        boolean isToCamel = false;
        if (underlineToCaseCamel.length > 0) {
            isToCamel = underlineToCaseCamel[0];
        }


        // 1. 获取处理语句
        PreparedStatement ps = conn.prepareStatement(sql);

        // 2. 给占位符赋值
        for (int i = 0; args != null && i < args.length; i++) {
            Object arg = args[i];
            ps.setObject(i + 1, arg);
        }

        // 3. 执行查询
        // resultSet: 结果集，类似一个指针，没有指向任何位置，查询到十行数据，用next方法获取下一行数据，直到没有数据为止
        ResultSet resultSet = ps.executeQuery();
        // 获取表的元数据，和表的列数
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // 4. 吧查询到的结果封装到list集合中
        while (resultSet.next()) { // 表示有一行数据，并把指针移动到这行数据上
            // 只要能进来，就表示读取到一行数据
            // 把这行数据所有的列全部读出来，封装到一个T类型的对象中
            // 泛型不能直接new 只能通过反射
            T t = tClass.newInstance();

            // 遍历这行每一列
            for (int i = 1; i <= columnCount; i++) {
                // 读取列名(这个方法取得是别名)
                String columnName = metaData.getColumnLabel(i);

                // 读取列值
                Object columnValue = resultSet.getObject(i);

                System.out.println("columnName: " + columnName + ", columnValue: " + columnValue);

                // 判断一下是不是要下划线转驼峰，因为从表里获得的列名一般是下划线命名的而bean的属性是驼峰命名的
                if (isToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                }

                // 把列名和列值封装到T对象的同名属性（pojo），或者做成key——value进行赋值
                BeanUtils.setProperty(t, columnName, columnValue);
            }
            result.add(t);
        }
        return result;
    }
    // 测试queryList方法
    public static void main(
            String[] args) throws InvocationTargetException, SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        List<JSONObject> list = queryList(getPhoenixConnection(), "select * from dim_sku_info where id=?", new Object[]{"1"}, JSONObject.class);
//        List<TableProcess> list = queryList(getJdbcConnection("com.mysql.cj.jdbc.Driver", "jdbc:mysql://hadoop162:3306/gmall_config?useSSL=false", "root", "aaaaaa"),
//                "select * from table_process",
//                null,
//                TableProcess.class,
//                true);
        for (JSONObject object : list) {

            System.out.println(object);
        }
    }
}
