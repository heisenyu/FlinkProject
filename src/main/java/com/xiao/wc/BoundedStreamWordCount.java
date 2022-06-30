package com.xiao.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author : Kuangjm
 * @Time : 2022/6/29 21:50
 * @Project : FlinkProject
 * @Version : 1.0
 */
public class BoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        //1. 创建流式的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 读取文件
        DataStreamSource<String> lineDSS = env.readTextFile("input/words.txt");

        //3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //wordAndOne.print();

        //4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneUG = wordAndOne.keyBy(tuple -> tuple.f0);
        //wordAndOneUG.print();


        //5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        sum.print();

        //6. 执行
        env.execute();


    }
}
