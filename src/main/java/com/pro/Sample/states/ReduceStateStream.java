package com.pro.Sample.states;

import com.pro.Pojo.Event;
import com.pro.Source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class ReduceStateStream {
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
        //统计每个10s窗口内的url的pv
        stream.keyBy(value -> value.action)
                .process(
                        new KeyedProcessFunction<String, Event, String>() {
                            public MapState<String, Integer> mapState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                mapState = getRuntimeContext().getMapState(
                                        new MapStateDescriptor<String, Integer>("map", String.class,Integer.class))
                                ;
                            }

                            @Override
                            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                                //数据进入之后，存入mapState中。key存行为，value存数据，并以定时器是否已经开启
                                if (mapState.contains(value.action)){
                                    mapState.put(value.action,mapState.get(value.action)+1);
                                }else{
                                    //定时并初始化
                                    mapState.put(value.action,1);
                                    ctx.timerService().registerEventTimeTimer(value.times+5000);
                                }
                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                Map.Entry<String, Integer> next = mapState.entries().iterator().next();
                                out.collect("result=>"+next.getKey()+"="+next.getValue());
                                //清空
                                mapState.clear();
                            }
                        }
                ).print()
                ;

        env.execute();



    }
}
