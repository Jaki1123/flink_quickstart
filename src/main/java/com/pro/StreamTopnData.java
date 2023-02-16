package com.pro;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class StreamTopnData {
  public static void main(String[] args) throws Exception {
          //1.增加环境
          StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
          env.setParallelism(1);
          //2.加载数据源
          //设置水位线？？
          SingleOutputStreamOperator<Event> eventSingleOutputStreamOperator = env.addSource(new ClickRoundSource()).assignTimestampsAndWatermarks(
                  WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                          .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.times)
          );

      //3.进行数值计算
      eventSingleOutputStreamOperator
                  .keyBy(value -> true)
                          .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                            .aggregate(new newAggFunction())
              .print();




          //3.计算每个网站登录最多的topN，并把流数据存入mysql
//          sourcedata.addSink(JdbcSink.sink(
//                  "insert into eventinfo (user,action,times) values (?,?,?)"
//                  ,(statement, event) -> {
//                      statement.setString(1,event.user);
//                      statement.setString(2,event.action);
//                      statement.setString(3, String.valueOf(event.times));
//                  }
//                  , JdbcExecutionOptions.builder()
//                          .withBatchSize(1000)
//                          .withBatchIntervalMs(200)
//                          .withMaxRetries(5)
//                          .build()
//                  ,new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                          .withUrl("jdbc:mysql://rm-uf612919l8r784nql.mysql.rds.aliyuncs.com:3306/onedata_dev?verifyServerCertificate=false&useSSL=false")
//                          .withDriverName("com.mysql.jdbc.Driver")
//                          .withUsername("")
//                          .withPassword("")
//                          .build()
//          ));
          env.execute();
  }
    public static class newAggFunction implements AggregateFunction<Event,ArrayList<HashMap<String,Integer>>,ArrayList<HashMap<String,Integer>>>{

        @Override
        public ArrayList<HashMap<String, Integer>> createAccumulator() {
            return new ArrayList<HashMap<String,Integer>>();
        }

        @Override
        public ArrayList<HashMap<String, Integer>> add(Event value, ArrayList<HashMap<String, Integer>> accumulator) {
            Iterator<HashMap<String, Integer>> iterator = accumulator.iterator();
            int tag = 0;
            while (iterator.hasNext()){
                HashMap<String, Integer> next = iterator.next();
                if (next.get(value.action)!=null){
                    //若流进来的数据并是已有这个map中的数据，相加且结束循环
                    tag=1;
                    next.put(value.action, next.get(value.action)+1);
                }
                ;
            }
            if (tag==0){
                //若循环一遍还没有这个map，直接在list中添加
                HashMap<String, Integer> stringIntegerHashMap = new HashMap<>();
                stringIntegerHashMap.put(value.action, 1);
                accumulator.add(stringIntegerHashMap);
            }
            return accumulator;
        }

        @Override
        public ArrayList<HashMap<String, Integer>> getResult(ArrayList<HashMap<String, Integer>> accumulator) {
            accumulator.sort((o1, o2) -> Integer.valueOf((String) o1.values().toArray()[0])>=Integer.valueOf((String) o2.values().toArray()[0])?1:-1);
            ArrayList<HashMap<String, Integer>> hashMaps = new ArrayList<>();
            for (int i=1;i<=2;i++){
                hashMaps.add(accumulator.get(i));
            }
            return hashMaps;
        }

        @Override
        public ArrayList<HashMap<String, Integer>> merge(ArrayList<HashMap<String, Integer>> a, ArrayList<HashMap<String, Integer>> b) {
            return null;
        }
    }

    public static class newAggAllFunction  extends ProcessWindowFunction<Event,String,Boolean,TimeWindow>{

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            //输出窗口数据
            out.collect("窗口"+context.window().getStart()+"~"+context.window().getEnd()+"中共有"+elements.spliterator().getExactSizeIfKnown()+"元素，水位线："+context.currentWatermark()+":");
        }
    }

}
