package com.xiao.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : Kuangjm
 * @Time : 2022/7/2 1:11
 * @Project : FlinkProject
 * @Version : 1.0
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());

        eventDataStreamSource.print("single");

        env.execute();

    }
}
