package com.pro;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;

public class BatchWordCount {
  public static void main(String[] args) throws Exception {
     //1.创建执行环境
      ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
      //2.读取文件
      DataSource<String> stringDataSource = executionEnvironment.readTextFile("./src/main/resources/words.txt");
      //3.转换数据格式为一个单词一个1
      FlatMapOperator<String, Tuple2<String, Long>> returns = stringDataSource.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
          for (String s : value.split(" ")) {
              out.collect(Tuple2.of(s, Long.valueOf(1)));
          }
      }).returns(Types.TUPLE(Types.STRING, Types.LONG));
      //4.按String进行分组,按数字聚合
      AggregateOperator<Tuple2<String, Long>> sum = returns.groupBy(0).sum(1);
      sum.print();
  }
}
