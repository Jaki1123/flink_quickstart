package com.pro;

import com.pro.Pojo.Event;
import com.pro.Source.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TimeUtils;

public class StreamData {
  public static void main(String[] args) throws Exception {
     //创建执行环境
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      //设置并行度
      env.setParallelism(1);
      //数据源
      DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
    // flatmap
      SingleOutputStreamOperator<Tuple2<String, String>> returns = eventDataStreamSource
              .flatMap(
                      (FlatMapFunction<Event, Tuple2<String, String>>)
                              (value, out) -> {
                                  out.collect(Tuple2.of("user", value.user));
                                  out.collect(Tuple2.of("action", value.action));
                                  out.collect(Tuple2.of("times", String.valueOf(value.times)));
                              })
              .returns(Types.TUPLE(Types.STRING, Types.STRING));

      returns.filter(value -> value.f0.equals("user"))
              .map(nametuple -> Tuple2.of(nametuple.f1,1)).returns(Types.TUPLE(Types.STRING,Types.INT))
              .keyBy(value -> value.f0)
                      .reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> Tuple2.of(value1.f0,Integer.valueOf(value2.f1+value1.f1))).print();


//      map
      SingleOutputStreamOperator<String> map = eventDataStreamSource.map((MapFunction<Event, String>) value -> value.user);
//      fiter
      SingleOutputStreamOperator<String> filterresult = map.filter((FilterFunction<String>) value -> value.equals("Duzhixin"));

      env.execute();
  }
}
