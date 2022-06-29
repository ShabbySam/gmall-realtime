package com.myPractice.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : 小嘘嘘
 * @create 2022/6/24 19:49
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeywordBean {
    private String stt;
    private String edt;
    private String source;
    private String keyword;
    private Long keywordCount;
    private Long ts;
}
