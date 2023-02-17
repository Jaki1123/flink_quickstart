package com.pro.Source;

import com.pro.Pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickRoundSource implements SourceFunction<Event> {
    private boolean runnning = true;
    @Override
    public void run(SourceContext ctx) throws Exception {
        Random random = new Random();
        String[] user = {"ZhangJiaqi","LiYang","Liuliang","Chenyi","Qushuxian","Duzhixin"};
        String[] action = {"点击","浏览","说哈哈哈哈","数数","数鸭子","买东西","阿巴阿巴"};
        while (runnning){
          //判断这次进入几条数据1-5条
            for(int i=1;i<=random.nextInt(2)+1;i++){
                ctx.collect(
                        new Event(user[random.nextInt(user.length)]
                                , action[random.nextInt(action.length)]
                                //模仿数据延迟：当随机数>=0.9时，数据延迟0.5秒到达
                                ,random.nextDouble()>=0.8?Calendar.getInstance().getTimeInMillis()-random.nextInt(5000):Calendar.getInstance().getTimeInMillis())
                );
            }
        Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        runnning = false;

    }
}
