package com.pro.Sample.mutistream;

import com.pro.Pojo.ActionAggInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Iterator;

public class MutiCoProcessData {
    //实现实时对账需求，即APP的支付操作和第三方的支付操作的一个双流Join。App的支付事件和第三方的支付时间会相互等待5s，如果等不来对应的支付事件，则输出报警信息。
    //App支付数据信息如：`Tuple3.of("order-1","app",1000)`
    //三方支付数据信息如：`Tuple3.of("order-1","third","success",2000)`
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建输入源头数据
        DataStreamSource<Tuple3<String, String, Long>> streamfromapp = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3000L)
        );
        DataStreamSource<Tuple4<String, String, String, Long>> streamfromthird = env.fromElements(
                Tuple4.of("order-1", "third", "success", 2000L),
                Tuple4.of("order-2", "third", "success", 4000L),
                Tuple4.of("order-3", "third", "success", 10000L),
                Tuple4.of("order-6", "third", "success", 5000L)
        );
        //分配水位线和时间取值
        streamfromapp.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String,String,Long>>) (element, recordTimestamp) -> element.f2))
        ;
        streamfromthird.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String,String,String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple4<String, String, String, Long>>) (element, recordTimestamp) -> element.f3)
        )
        ;

    // 合流
    streamfromapp.connect(streamfromthird)
        .keyBy(value -> value.f0, value -> value.f0)
        .process(new CoProcessFunction<
                Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
              // 创建不同的状态集，ListState
              private ListState<Tuple3<String, String, Long>> applistState;
              private ListState<Tuple4<String, String, String, Long>> thirdlistState;

              @Override
              public void open(Configuration parameters) throws Exception {
                // 初始化
                applistState =
                    getRuntimeContext()
                        .getListState(
                            new ListStateDescriptor<Tuple3<String, String, Long>>(
                                "appliststate", Types.TUPLE()));
                thirdlistState =
                    getRuntimeContext()
                        .getListState(
                            new ListStateDescriptor<Tuple4<String, String, String, Long>>(
                                "thirdliststate", Types.TUPLE()));
              }

              @Override
              public void processElement1(Tuple3<String, String, Long> value,CoProcessFunction<Tuple3<String, String, Long>,Tuple4<String, String, String, Long>,String>.Context ctx,Collector<String> out) throws Exception {
                // 判断thirdlistState里是否有数据，如果有，说明等到了
                Iterator<Tuple4<String, String, String, Long>> iterator =thirdlistState.get().iterator();
                  applistState.add(value);
                  if (!iterator.hasNext()) {
                      // 如果没有，开启定时器,把数据加入list,交给ontimer处理
                      ctx.timerService().registerEventTimeTimer(5000);
                  } else {
                      //如果有数据，判断数据是否差5s
                }
              }

              @Override
              public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                // 判断appliststate中是否有数据，如果有，说明ok
                Iterator<Tuple3<String, String, Long>> iterator = applistState.get().iterator();
                  thirdlistState.add(value);
                  if (!iterator.hasNext()) {
                      ctx.timerService().registerEventTimeTimer(5000);
                } else {
                      //如果有数据，判断数据是否差5s
                }
              }

              @Override
              public void onTimer(long timestamp,CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx,Collector<String> out)
                  throws Exception {
                if (!applistState.get().iterator().hasNext()) {
                    out.collect("app数据丢失,只有三方账号数据"+thirdlistState.get().iterator().next().toString());
                } else if (!thirdlistState.get().iterator().hasNext()) {
                  out.collect("三方数据丢失,只有app端数据"+applistState.get().iterator().next().toString());
                }else{
                    //判断数据是否差5s
                    out.collect("等到数据了:app数据："+applistState.get().iterator().next().toString()+"third数据:"+thirdlistState.get().iterator().next().toString());
                }
              }
            })
            .print();


    env.execute();

    }

}
