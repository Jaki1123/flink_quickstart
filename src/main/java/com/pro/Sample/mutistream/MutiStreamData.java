package com.pro.Sample.mutistream;

import com.pro.Pojo.Event;
import com.pro.Source.ClickRoundSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MutiStreamData {
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
        SingleOutputStreamOperator<String> stream1 = datastream.process(new ProcessFunction<Event, String>() {
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

        //开始输出主流和侧输出流
        stream1.print("主流数据");
        stream1.getSideOutput(outputtag1).print("侧输出流1数据");
        stream1.getSideOutput(outputtag2).print("侧输出流2数据");
        env.execute();
    }
}
