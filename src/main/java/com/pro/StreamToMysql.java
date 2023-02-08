package com.pro;

import com.pro.Pojo.Event;
import com.pro.Source.ClickSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamToMysql {
  public static void main(String[] args) throws Exception {
    //1.获取上下文环境
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      //2.获取Source数据
      DataStreamSource<Event> eventDataStreamSource = env.addSource(new ClickSource());
    // 3.加工数据
      SingleOutputStreamOperator<Tuple2<String, Integer>> usercounts = eventDataStreamSource.map(value -> Tuple2.of(value.user, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
              .keyBy(value -> value.f0)
              .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1)).returns(Types.TUPLE(Types.STRING, Types.INT));
      //4.将数据导入mysql
      usercounts.addSink(
              JdbcSink.sink(
                      "insert into usercount (username,count) values (?,?)"
                      , (statement, values) -> {
                          statement.setString(1, values.f0);
                          statement.setInt(2, values.f1);
                      }
                      , JdbcExecutionOptions.builder()
                              .withBatchSize(1000)
                              .withBatchIntervalMs(200)
                              .withMaxRetries(5)
                              .build()
                      , new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                              .withUrl("jdbc:mysql://rm-uf612919l8r784nql.mysql.rds.aliyuncs.com:3306/onedata_dev?verifyServerCertificate=false&useSSL=false")
                              .withDriverName("com.mysql.jdbc.Driver")
                              .withUsername("")
                              .withPassword("")
                              .build()

              )
      );
      env.execute();

  }
}
