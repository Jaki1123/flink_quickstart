package com.pro.Sample.mutistream;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MutiCoProcessData {
    //实现实时对账需求，即APP的支付操作和第三方的支付操作的一个双流Join。App的支付事件和第三方的支付时间会相互等待5s，如果等不来对应的支付事件，则输出报警信息。
    //App支付数据信息如：`Tuple3.of("order-1","app",1000)`
    //三方支付数据信息如：`Tuple3.of("order-1","third","success",2000)`
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建输入源头数据
        DataStreamSource<Tuple3<String, String, Integer>> streamfromapp = env.fromElements(
                Tuple3.of("order-1", "app", 1000),
                Tuple3.of("order-2", "app", 2000),
                Tuple3.of("order-3", "app", 3000)
        );
        DataStreamSource<Tuple4<String, String, String, Integer>> streamfromthird = env.fromElements(
                Tuple4.of("order-1", "third", "success", 2000),
                Tuple4.of("order-2", "third", "success", 4000),
                Tuple4.of("order-5", "third", "success", 6000),
                Tuple4.of("order-6", "third", "success", 5000)
        );
        //分配水位线和时间取值

        //

    }

}
