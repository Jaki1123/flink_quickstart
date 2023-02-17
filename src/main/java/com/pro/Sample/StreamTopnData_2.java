package com.pro.Sample;

import com.pro.Pojo.ActionAggInfo;
import com.pro.Pojo.Event;
import com.pro.Source.ClickRoundSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;


//两次分组，第一次分组接窗口，第二次分组后接分组函数
public class StreamTopnData_2 {
  public static void main(String[] args) throws Exception {
          //1.增加环境
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          //2.加载数据源
          //设置水位线
          //★：延迟三秒，即flink时间上，当数据时间-3秒刚刚大于等于窗口结束时间时，进行窗口计算。
          //★：水位线来一条生成一条
          SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = env.addSource(new ClickRoundSource()).assignTimestampsAndWatermarks(
                  WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                          .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.times)
          );
      eventSingleOutputStreamOperator.print();

      //3.进行数值计算
      SingleOutputStreamOperator<ActionAggInfo> aggregateByKey = eventSingleOutputStreamOperator
              .keyBy(value -> value.action)
              .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
              .aggregate(new newAggFunction(), new newAggAllFunction());

      aggregateByKey
              .keyBy(value -> value.start)
                      .process(new newAggWithWindowFunction())
              .print();
      env.execute();
  }
    public static class newAggFunction implements AggregateFunction<Event,HashMap<String,Integer>,HashMap<String,Integer>>{

        @Override
        public HashMap<String, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Integer> add(Event value, HashMap<String, Integer> accumulator) {
            if(accumulator.containsKey(value.action)){
                accumulator.put(value.action, accumulator.get(value.action)+1);
            }else {
                accumulator.put(value.action, 1);
            }
              return accumulator;
        }

        @Override
        public HashMap<String, Integer> getResult(HashMap<String, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public HashMap<String, Integer> merge(HashMap<String, Integer> a, HashMap<String, Integer> b) {
            return null;
        }
    }

    public static class newAggAllFunction  extends ProcessWindowFunction<HashMap<String,Integer>,ActionAggInfo,String,TimeWindow>{
        @Override
        public void process(String s, ProcessWindowFunction<HashMap<String, Integer>, ActionAggInfo, String, TimeWindow>.Context context, Iterable<HashMap<String, Integer>> elements, Collector<ActionAggInfo> out) throws Exception {
            //窗口开始时间
            long start = context.window().getStart();
            //窗口结束时间
            long end = context.window().getEnd();
            //聚合窗口结果值
            Integer integer = elements.iterator().next().get(s);
            out.collect(new ActionAggInfo(s,integer,start,end));
        }
    }


    public static class newAggWithWindowFunction extends KeyedProcessFunction<Long, ActionAggInfo, ArrayList<ActionAggInfo>> {
        //用构造器可以实现赋值
      private ListState<ActionAggInfo> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //???从环境中获取列表状态句柄？？？
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ActionAggInfo>("url-view-count-list"
                            , Types.POJO(ActionAggInfo.class))
            );
        }

        @Override
        public void processElement(ActionAggInfo value, KeyedProcessFunction<Long, ActionAggInfo, ArrayList<ActionAggInfo>>.Context ctx, Collector<ArrayList<ActionAggInfo>> out) throws Exception {
            //把数据存入列表状态
            listState.add(value);
            // 注册定时器,延迟一毫秒，将所有数据
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey()+1);
        }
        //到达时间后将数据进行计算
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, ActionAggInfo, ArrayList<ActionAggInfo>>.OnTimerContext ctx, Collector<ArrayList<ActionAggInfo>> out) throws Exception {
            ArrayList<ActionAggInfo> actionAggInfos = new ArrayList<>();
            for (ActionAggInfo value : listState.get()) {
                actionAggInfos.add(value);
            }
            listState.clear();
            actionAggInfos.sort((o1,o2)->o2.actioninfo-o1.actioninfo);
            if (actionAggInfos.size()>=2){
                ArrayList<ActionAggInfo> actionAggInfos2 = new ArrayList<>();
                actionAggInfos2.addAll(actionAggInfos.subList(0, 2));
                out.collect(actionAggInfos2);
            }else{
                out.collect(actionAggInfos);
            }
        }
    }

}
