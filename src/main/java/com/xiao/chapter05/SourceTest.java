package com.xiao.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Calendar;

/**
 * @Author : Kuangjm
 * @Time : 2022/7/2 0:49
 * @Project : FlinkProject
 * @Version : 1.0
 */
public class SourceTest {
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//方便测试，保证顺序


        // 1. 从文件中读取数据
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/clicks.txt");


        // 2. 从集合中读取数据（测试）
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", Calendar.getInstance().getTimeInMillis()));
        events.add(new Event("Bob", "./home", Calendar.getInstance().getTimeInMillis()));

        DataStreamSource<Event> fromCollection = env.fromCollection(events);


        // 3. 从元素读取数据（测试）
        DataStreamSource<Event> fromElements = env.fromElements(
                new Event("Mary", "./home", Calendar.getInstance().getTimeInMillis()),
                new Event("Bob", "./home", Calendar.getInstance().getTimeInMillis())
        );


        // 4. 从socket文本流中读取（测试）
        //DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

    }
}
