package com.pro.Source;

import com.pro.Pojo.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private boolean runnning = true;
    @Override
    public void run(SourceContext ctx) throws Exception {
        Random random = new Random();
        String[] user = {"ZhangJiaqi","LiYang","Liuliang","Chenyi","Qushuxian","Duzhixin"};
        String[] action = {"点击","浏览","说哈哈哈哈","数数","数鸭子","买东西","阿巴阿巴"};
        while (runnning){
      ctx.collect(
          new Event(user[random.nextInt(user.length)], action[random.nextInt(action.length)], Calendar.getInstance().getTimeInMillis())
      );
      Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        runnning = false;

    }
}
