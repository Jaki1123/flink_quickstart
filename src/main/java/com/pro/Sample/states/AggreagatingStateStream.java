package com.pro.Sample.states;

import com.pro.Pojo.Event;
import com.pro.Source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class AggreagatingStateStream {
    /*
    对每五个用户事件统计一次平均时间戳。类似计数窗口COuntWindow求平均值.
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.times;
                            }
                        })
        );
        //
        stream.keyBy(value -> value.user)
                .flatMap(new RichFlatMapFunction<Event, String>() {
                    //定义集合状态，用来计算平均时间戳
                    AggregatingState<Event,Long> aggregatingState;
                    //定义一个值的状态，用来保存当前用户访问频次
                    ValueState<Long> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aggregatingState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                                        "agg",
                                        new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                                            @Override
                                            public Tuple2<Long, Long> createAccumulator() {
                                                return Tuple2.of(0L, 0L);
                                            }

                                            @Override
                                            public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                                                return Tuple2.of(accumulator.f0 + value.times, accumulator.f1 + 1);
                                            }

                                            @Override
                                            public Long getResult(Tuple2<Long, Long> accumulator) {
                                                return accumulator.f0 / accumulator.f1;
                                            }

                                            @Override
                                            public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                                                return null;
                                            }
                                        }
                                        , Types.TUPLE(Types.LONG, Types.LONG)
                                )
                        );

                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));

                    }

                    @Override
                    public void flatMap(Event value, Collector<String> out) throws Exception {
                        Long value1 = valueState.value();
                        if (value1==null){
                            value1 = 1L;
                        }else {
                            value1++;
                        }
                        valueState.update(value1);
                        aggregatingState.add(value);

                        //达到五次就清空状态并输出结果
                        if (value1==5){
                            out.collect(value.user+"平均时间戳"+new Timestamp(aggregatingState.get()));
                            valueState.clear();
                        }



                    }
                })
                .print();

        env.execute();
    }
}
