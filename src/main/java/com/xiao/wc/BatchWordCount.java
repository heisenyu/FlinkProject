package com.xiao.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author : Kuangjm
 * @Time : 2022/6/28 0:24
 * @Project : FlinkProject
 * @Version : 1.0
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.从文件中读取数据(按行读)
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");
        lineDataSource.print();

        // 3.将每行数据进行分词，转换成二元组类型
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));
        //wordAndOne.print();

        // 4.按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);


        // 5.分组内进行聚合
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        sum.print();
    }
}
