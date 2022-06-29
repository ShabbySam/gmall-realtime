import com.myPractice.realtime.bean.KeywordBean;

import java.lang.reflect.Field;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/24 19:15
 */
public class reflection {
    /**
     * reflection反射的复习
     *  几种方式获取class
     *      1. Class.forName("类名")
     *      2. 类名.class
     *      3. 对象.getClass
     */
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        // 正常使用
        KeywordBean bean = new KeywordBean();
        bean.setStt("abc");


        // 类名.class
        Class<KeywordBean> tClass = KeywordBean.class;
//        Field[] fields = tClass.getFields(); // 获取类中所有的 public 属性
        Field[] fields = tClass.getDeclaredFields(); // 获取类中所有的属性
        for (Field field : fields) {
            System.out.println(field.getName());
        }
        System.out.println("-------------------------");
        // 反射获取属性
        Field stt = tClass.getDeclaredField("stt");
        stt.setAccessible(true); // 设置允许访问私有属性
        Object o = stt.get(bean); // 获取这个属性所在对象的值
        System.out.println(o); // 输出abc

        System.out.println("-------------------------");
        stt.set(bean,"hello"); //直接给KeywordBean所属的对象bean的stt属性赋值hello
        System.out.println(bean.getStt());


    }
}
