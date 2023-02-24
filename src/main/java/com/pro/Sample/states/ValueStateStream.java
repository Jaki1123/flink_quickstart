package com.pro.Sample.states;

import com.pro.Pojo.Event;
import com.pro.Source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ValueStateStream {
    /*
    使用用户id进行分流，分别统计每个用户的pv数据。
    新：并不想每次pv加一后就将统计结果发送至下游，所以应该注册定时器，用来隔一段时间发送一个统计结果
    功能点：
    1.统计每个用户的pv数据
    2.每段时间输出一次，在发送下游。注册定时器，且判断定时器是否生效
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream1 = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = stream1.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.times)
        );

        SingleOutputStreamOperator<String> keyname = eventSingleOutputStreamOperator.keyBy(value -> value.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private ValueState<Integer> valuestate;
                    private ValueState<Integer> timestate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valuestate = getRuntimeContext().getState(
                                new ValueStateDescriptor<Integer>("keyname", Types.INT)
                        );
                        timestate = getRuntimeContext().getState(
                                new ValueStateDescriptor<Integer>("dingshiqi", Types.INT)
                        );
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        //来一条数据后，把数据加入state
                        if (valuestate.value() != null) {
                            valuestate.update(valuestate.value() + 1);
                        } else {
                            valuestate.update(1);
                        }
                        out.collect(value.toString());
                        //判断定时器是否还生效
                        if (timestate.value()==null){
                            ctx.timerService().registerEventTimeTimer(value.times + 10000);
                            timestate.update(1);
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

                        out.collect("定时器"+ctx.getCurrentKey() + "现在的次数是:" + valuestate.value() + "时间" + ctx.timestamp());
                        timestate.clear();
                    }
                });
        keyname.print();
        env.execute();

    }
}
