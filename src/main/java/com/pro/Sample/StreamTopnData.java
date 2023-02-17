package com.pro.Sample;

import com.pro.Pojo.Event;
import com.pro.Source.ClickRoundSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//来一条数据计算一次，任务重
public class StreamTopnData {
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
      eventSingleOutputStreamOperator
                  .keyBy(value -> true)
                          .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                            .aggregate(new newAggFunction(),new newAggAllFunction())
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
            Iterator<Map.Entry<String, Integer>> iterator = accumulator.entrySet().iterator();
            int tag=0;
            //判断value是否再HashMap中出现过，如果出现value+1
              while (iterator.hasNext()){
                  Map.Entry<String, Integer> next = iterator.next();
                  if (next.getKey().equals(value.action)){
                      accumulator.put(value.action, next.getValue()+1);
                      tag=1;
                      break;
                  }
              }
              if(tag==0){
                  //标识这个value值第一次出现在hashmap中,直接添加
                  accumulator.put(value.action, 1);
              }
              return accumulator;
        }

        @Override
        public HashMap<String, Integer> getResult(HashMap<String, Integer> accumulator) {
            //对累加器中的HashMap进行排序，取前2
            //map转换为list
            List<Map.Entry<String, Integer>> collect = accumulator.entrySet().stream().collect(Collectors.toList());
            collect.sort((o1,o2)->o1.getValue()<o2.getValue()?1:-1);
            //讲list转换为Map
            if(collect.size()>=2){
                Map<String, Integer> collect1 = collect.subList(0, 2).stream().collect(Collectors.toMap(Map.Entry<String, Integer>::getKey, Map.Entry<String, Integer>::getValue));
                HashMap<String, Integer> accumulator2 = new HashMap<>();
                accumulator2.putAll(collect1);
                return accumulator2;
            }
            return accumulator;

        }

        @Override
        public HashMap<String, Integer> merge(HashMap<String, Integer> a, HashMap<String, Integer> b) {
            return null;
        }
    }

    public static class newAggAllFunction  extends ProcessWindowFunction<HashMap<String,Integer>,String,Boolean,TimeWindow>{

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<HashMap<String, Integer>, String, Boolean, TimeWindow>.Context context, Iterable<HashMap<String, Integer>> elements, Collector<String> out) throws Exception {
            out.collect("窗口"+context.window().getStart()+"~"+context.window().getEnd()+"中共有"+elements.spliterator().getExactSizeIfKnown()+"元素，水位线："+context.currentWatermark()+":"+elements.iterator().next());
        }
    }

}
