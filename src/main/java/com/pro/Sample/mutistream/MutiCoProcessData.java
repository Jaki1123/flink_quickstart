package com.pro.Sample.mutistream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

public class MutiCoProcessData {
    //实现实时对账需求，即APP的支付操作和第三方的支付操作的一个双流Join。App的支付事件和第三方的支付时间会相互等待5s，如果等不来对应的支付事件，则输出报警信息。
    //App支付数据信息如：`Tuple3.of("order-1","app",1000)`
    //三方支付数据信息如：`Tuple3.of("order-1","third","success",2000)`
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为1
//        env.setParallelism(1);
        //创建输入源头数据
        DataStreamSource<Tuple3<String, String, Long>> streamfromapp = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 2000L),
                Tuple3.of("order-3", "app", 3000L)
        );
        DataStreamSource<Tuple4<String, String, String, Long>> streamfromthird = env.fromElements(
                Tuple4.of("order-3", "third", "success", 10000L),
                Tuple4.of("order-1", "third", "success", 3000L),
                Tuple4.of("order-2", "third", "success", 4000L),
                Tuple4.of("order-6", "third", "success", 5000L)
        );
        //分配水位线和时间取值
        SingleOutputStreamOperator<Tuple3<String, String, Long>> streamfromapp1 = streamfromapp.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String,String,Long>>) (element, recordTimestamp) -> element.f2))
        ;
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> streamfromthird1 = streamfromthird.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String,String,String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple4<String, String, String, Long>>) (element, recordTimestamp) -> element.f3)
        )
        ;
        // 合流
        streamfromapp1.connect(streamfromthird1)
        .keyBy(value -> value.f0, value -> value.f0)
        .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>() {
              // 定义状态变量
              private ValueState<Tuple3<String, String, Long>> appvaluestate;
              private ValueState<Tuple4<String, String, String, Long>> thirdvaluestate;

              @Override
              public void open(Configuration parameters) throws Exception {
                  // 初始化
                  appvaluestate = getRuntimeContext().getState(
                          new ValueStateDescriptor<Tuple3<String, String, Long>>("appliststate", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                  );
                  thirdvaluestate = getRuntimeContext().getState(
                          new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdliststate", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
                  );
              }

              @Override
              public void processElement1(Tuple3<String, String, Long> value,CoProcessFunction<Tuple3<String, String, Long>,Tuple4<String, String, String, Long>,String>.Context ctx,Collector<String> out) throws Exception {
                  out.collect("时间:数据"+value+"到达processElement1时间"+ctx.timestamp());
                  if (thirdvaluestate.value()!=null){
                      // 判断thirdvaluestate里是否有数据，如果有，说明成功
                      out.collect("数据正确:"+value+" "+thirdvaluestate.value());
                      //清空状态，如果不清空，在定时器里面数据会再出来一次
                      thirdvaluestate.clear();
                  }else{
                      //如果thirdvalueState里面没有数据，则促发定时器
                      //更新数据
                      appvaluestate.update(value);
                      ctx.timerService().registerEventTimeTimer(value.f2+5000);
                  }
              }

              @Override
              public void processElement2(Tuple4<String, String, String, Long> value, CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
                  out.collect("时间:数据"+value+"到达processElement2时间"+ctx.timestamp());
                  if (appvaluestate.value()!=null){
                      // 判断appvaluestate里是否有数据，如果有，说明成功
                      out.collect("数据正确:"+value+" "+appvaluestate.value());
                      //清空状态，如果不清空，在定时器里面数据会再出来一次
                      appvaluestate.clear();
                  }else{
                      //如果appvaluestate里面没有数据，则促发定时器
                      //更新数据
                      thirdvaluestate.update(value);
                      ctx.timerService().registerEventTimeTimer(value.f3+5000);
                  }
              }

              @Override
              public void onTimer(long timestamp,CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String>.OnTimerContext ctx,Collector<String> out) throws Exception {
                  out.collect("appvaluestate:"+appvaluestate.value()+"thirdvaluestate:"+thirdvaluestate.value()+"触发器时间"+ctx.timestamp());
                  //触发定时器，看数据是否存在
                  if (appvaluestate.value()!=null){
                      //appvaluestate里面没有数据
                      out.collect("三方数据缺失,app数据为:"+appvaluestate.value());
                  }
                  if (thirdvaluestate.value()!=null){
                      out.collect("app数据缺失，third数据为:"+thirdvaluestate.value());
                  }
                  appvaluestate.clear();
                  thirdvaluestate.clear();
              }
            })
            .print();


    env.execute();

    }

}
