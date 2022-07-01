package com.xiao.chapter05;

import java.sql.Timestamp;

/**
 * @Author : Kuangjm
 * @Time : 2022/7/2 0:42
 * @Project : FlinkProject
 * @Version : 1.0
 */
public class Event {
    public String user;
    public String url;
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
