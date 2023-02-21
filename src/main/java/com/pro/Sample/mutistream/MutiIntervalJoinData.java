package com.pro.Sample.mutistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class MutiIntervalJoinData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(
                Tuple3.of("Mary", "order-1", 5000L),
                Tuple3.of("Alice", "order-2", 5000L),
                Tuple3.of("Bob", "order-3", 20000L),
                Tuple3.of("Alice", "order-4", 20000L),
                Tuple3.of("Cary", "order-5", 51000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2)
        );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(
                Tuple3.of("Bob", "读书", 2000L),
                Tuple3.of("Alice", "看报", 3000L),
                Tuple3.of("Alice", "数鸭子", 3500L),
                Tuple3.of("Bob", "学画画", 2500L),
                Tuple3.of("Alice", "说哈哈哈", 36000L),
                Tuple3.of("Bob", "说额呵呵呵", 30000L),
                Tuple3.of("Bob", "看电视", 23000L),
                Tuple3.of("Bob", "玩电脑", 33000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2)
        );

        stream1.keyBy(value -> value.f0)
                .intervalJoin(stream2.keyBy(value -> value.f0))
                .between(Time.seconds(-5),Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context ctx, Collector<String> out) throws Exception {
                       out.collect(right+"=>"+left);
                    }
                })
                .print();
        env.execute();


    }
}
