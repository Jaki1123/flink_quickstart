package com.pro.Sample.states;

import com.pro.Pojo.Event;
import com.pro.Source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

public class OperatorStateStream {
    public static void main(String[] args) throws Exception {
        /*
        自定义sinkFunction在CheckPointedFunction中进行数据缓存，然后统一发送到下游。
        演示列表状态的平均分割重组
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.times;
                                    }
                                })
                );

        stream.print("input");

        //创建输出，批量缓存十个输出到控制台
        stream.addSink(new BufferSinkFunction(10));

        env.execute();

    }
    public static class BufferSinkFunction implements SinkFunction<Event>,CheckpointedFunction{
        private final int threshold;
        private transient ListState<Event> checkpointedState;
        private List<Event> bufferElements;

        public BufferSinkFunction(int threshold) {
            this.threshold = threshold;
            this.bufferElements = new ArrayList<>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            //往bufferlist中添加数据
            bufferElements.add(value);
            if (bufferElements.size()==threshold){
                for (Event bufferElement : bufferElements) {
                    //输出到系统外部，用控制台模拟打印
                    System.out.println(bufferElement);
                }
                System.out.println("=============输出完毕===============");
                bufferElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            //把当前局部变量中的所有元素写入检查点中
            for (Event bufferElement : bufferElements) {
                checkpointedState.add(bufferElement);
            }

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Event> buffer = new ListStateDescriptor<>("buffer", Types.POJO(Event.class));
            checkpointedState = context.getOperatorStateStore().getListState(buffer);

            //如果是从故障中恢复，九江ListState中的所有元素添加到局部变量中
            if (context.isRestored()){
                for (Event event : checkpointedState.get()) {
                    bufferElements.add(event);
                }
            }

        }


    }
}
