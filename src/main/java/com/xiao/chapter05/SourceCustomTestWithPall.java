package com.xiao.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * @Author : Kuangjm
 * @Time : 2022/7/2 1:11
 * @Project : FlinkProject
 * @Version : 1.0
 */
public class SourceCustomTestWithPall {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> eventDataStreamSource = env.addSource(new ParallelismSourceFunction()).setParallelism(4);

        eventDataStreamSource.print("double").setParallelism(2);

        env.execute();

    }

    public static class ParallelismSourceFunction extends RichParallelSourceFunction<Integer>{

        private boolean isRunning = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {

            Random random = new Random();

            while (isRunning){

                ctx.collect(random.nextInt());

                Thread.sleep(1000);

            }

        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
