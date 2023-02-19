package com.pro.Sample.mutistream;

import com.pro.Pojo.Event;
import com.pro.Source.ClickRoundSource;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MutiStreamData_2 {
    //分流
    public static void main(String[] args) throws Exception {
        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Event> datastream = env.addSource(new ClickRoundSource());

        //创建测输出流的标识。
        OutputTag<String> outputtag1 = new OutputTag<String>("tag1"){};
        OutputTag<String> outputtag2 = new OutputTag<String>("tag2"){};

        //开始侧输出流
        SingleOutputStreamOperator<String> stream = datastream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                if (value.user.equals("ZhangJiaqi")) {
                    ctx.output(outputtag1, value.toString());
                } else if (value.user.equals("LiYang")){
                    ctx.output(outputtag2, value.toString());
                }
                else {
                    out.collect(value.toString());
                }

            }
        });

        //得到两条输出流
        DataStream<String> stream1 = stream.getSideOutput(outputtag1);
        DataStream<String> stream2 = stream.getSideOutput(outputtag2);

        //输出流连接
        ConnectedStreams<String, String> connect = stream1.connect(stream2);
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String value) throws Exception {
                return "第一条流：" + value;
            }

            @Override
            public String map2(String value) throws Exception {
                return "第二条流：" + value;
            }
        });
        map.print();

        env.execute();
    }
}
