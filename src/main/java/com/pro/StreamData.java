package com.pro;

import com.pro.Pojo.Event;
import com.pro.Source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TimeUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class StreamData {
  public static void main(String[] args) throws Exception {
      //创建执行环境
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      //设置并行度
//      env.setParallelism(1);
      //获取数据源
      DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
      //水位线:有序流:当前最大时间戳
      eventDataStreamSource.assignTimestampsAndWatermarks(
              WatermarkStrategy.<Event>forMonotonousTimestamps()
                      .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.times)
      );
      //水位线:乱序流:延迟N秒获取数据
//      eventDataStreamSource.assignTimestampsAndWatermarks(
//              WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                      .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.times)
//      );
      //水位线:自定义水位线:重写WatermarkGenerator接口

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
      //流数据
      SingleOutputStreamOperator<Tuple2<String, Integer>> usercount = returns.filter(value -> value.f0.equals("user"))
              .map(nametuple -> Tuple2.of(nametuple.f1, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
              .keyBy(value -> value.f0)
              //时间窗口:滚动处理时间窗口,窗口大小5s
//              .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
              //时间窗口:滑动处理时间窗口，窗口大小10s，滑动长度5s
//              .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
              //时间窗口：会话处理时间窗口,会话间隙10s
//              .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
              //时间窗口：绘画处理时间窗口，动态配置会话间隙
//              .window(ProcessingTimeSessionWindows.withDynamicGap((SessionWindowTimeGapExtractor<Tuple2<String, Integer>>) element -> element.f0.length()*1000))
              //计数窗口：滚动计数窗口
//              .countWindow(10)
              //计数窗口：滑动计数窗口
              .countWindow(2,1)

              .reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> Tuple2.of(value1.f0, Integer.valueOf(value2.f1 + value1.f1)));






      //写入文件
      StreamingFileSink<String> fileSink = StreamingFileSink.forRowFormat(new Path("./output"), new SimpleStringEncoder<String>("UTF-8"))
              .withRollingPolicy(DefaultRollingPolicy.builder()
                      .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                      .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                      .withMaxPartSize(1024 * 1024 * 1024)
                      .build()
              )
              .build();
      usercount.map(value -> String.valueOf(value)).addSink(fileSink);
      //写入mysql

      env.execute();
  }
}
