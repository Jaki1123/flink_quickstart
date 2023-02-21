package com.pro.Sample.mutistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;


public class MutiWindowJoinData {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        //获取元素
        DataStreamSource<Tuple3<String, String, Long>> streamfromapp = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3000L)
        );
        DataStreamSource<Tuple4<String, String, String, Long>> streamfromthird = env.fromElements(
                Tuple4.of("order-1", "third", "success", 3000L),
                Tuple4.of("order-2", "third", "success", 4000L),
                Tuple4.of("order-2", "third", "success", 8000L),
                Tuple4.of("order-6", "third", "success", 5000L)
        );
        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamfromapp2 = streamfromapp.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (element, recordTimestamp) -> element.f2)
        );
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> streamfromthird2 = streamfromthird.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner((SerializableTimestampAssigner<Tuple4<String, String, String, Long>>) (element, recordTimestamp) -> element.f3)
        );


        streamfromapp2.join(streamfromthird2)
                .where(value -> value.f0)
                .equalTo(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
                    @Override
                    public String join(Tuple3<String, String, Long> first, Tuple4<String, String, String, Long> second) throws Exception {
                        return first.f0.equals(second.f0)?"数据匹配:"+first+"   "+second:"数据没有匹配上"+first+"   "+second;
                    }
                })
                .print()
                ;

        env.execute();
    }
}
