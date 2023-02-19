package com.pro.Sample.mutistream;

public class MutiCoProcessData {
    //实现实时对账需求，即APP的支付操作和第三方的支付操作的一个双流Join。App的支付事件和第三方的支付时间会相互等待5s，如果等不来对应的支付事件，则输出报警信息。
    //App支付数据信息如：`Tuple3.of("order-1","app",1000)`
    //三方支付数据信息如：`Tuple3.of("order-1","third","success",2000)`
    public static void main(String[] args) {

    }

}
