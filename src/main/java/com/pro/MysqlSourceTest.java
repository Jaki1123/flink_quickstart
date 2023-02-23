package com.pro;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MysqlSourceTest {
  public static void main(String[] args) throws Exception {
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname("jdbc:mysql://rm-uf612919l8r784nql.mysql.rds.aliyuncs.com")
            .port(3306)
            .databaseList("onedata_dev")
            .tableList("onedata_dev.eventinfo")
            .username("srv_onedata_dev")
            .password("vnmUUd$hNwg3")
            .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
            .build();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // enable checkpoint
    env.enableCheckpointing(3000);

    env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
            // set 4 parallel source tasks
            .setParallelism(4)
            .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

    env.execute("Print MySQL Snapshot + Binlog");
  }
}

