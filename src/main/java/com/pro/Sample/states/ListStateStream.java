package com.pro.Sample.states;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

public class ListStateStream {
    /*
    模拟实现FLinkSql中的：
    select * from A inner join B where A.id= B.id;
    用listStates
     */
    public static void main(String[] args) throws Exception {
        //准备两条流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        //合并成一条流才能处理
        streamfromapp2.keyBy(value -> value.f0)
                .connect(streamfromthird2.keyBy(value -> value.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
                    public ListState<Tuple3<String,String,Long>>  listStateapp;
                    public ListState<Tuple4<String, String, String, Long>>  listStatethird;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listStateapp=getRuntimeContext().getListState(new ListStateDescriptor<Tuple3<String,String,Long>>("app", Types.TUPLE()));
                        listStatethird=getRuntimeContext().getListState(new ListStateDescriptor<Tuple4<String, String, String, Long>>("third", Types.TUPLE()));
                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        //来一次输出一次
                        listStateapp.add(value);
                        Iterator<Tuple4<String, String, String, Long>> iterator = listStatethird.get().iterator();
                        while (iterator.hasNext()){
                            out.collect(value.toString()+"三方数据关联:"+iterator.next().toString());
                        }
                    }

                    @Override
                    public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        listStatethird.add(value);
                        Iterator<Tuple3<String, String, Long>> iterator = listStateapp.get().iterator();
                        while (iterator.hasNext()){
                            out.collect(value.toString()+"app数据关联:"+iterator.next().toString());

                        }

                    }

                })
                .print();



        env.execute();
    }
}
