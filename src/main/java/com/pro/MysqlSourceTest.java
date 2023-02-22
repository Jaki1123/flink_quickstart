package com.pro;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;

import java.util.Properties;

public class MysqlSourceTest {
  public static void main(String[] args) throws Exception {
    //
    Properties properties = new Properties();
//    properties.put("snapshot.locking.mode", "none");

    SourceFunction<String> source = MySqlSource.<String>builder()
            .hostname("jdbc:mysql://rm-.mysql.rds.aliyuncs.com")
            .port(3306)
            .databaseList("")
            .tableList("")
            .username("")
            .password("")
            .deserializer(new StringDebeziumDeserializationSchema())
//            .debeziumProperties(properties)
            .build();


    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(300);
    env.addSource(source)
            .print()
            .setParallelism(1)
            ;

    env.execute("mysqlcdc");
  }
}
