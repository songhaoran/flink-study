package com.song.flink.pv.bean;

import lombok.Data;

/**
 * Created by Song on 2019/12/25.
 */
@Data
public class UserVisitEvent {
    // 日志的唯一 id
    private String id;
    // 日期，如：20191025
    private String date;
    // 页面 id
    private Integer pageId;
    // 用户的唯一标识，用户 id
    private String userId;
    // 页面的 url
    private String url;
}
