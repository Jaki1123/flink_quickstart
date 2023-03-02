package com.pro.Source;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;


public class MysqlCDCDataSource {
  public static void main(String[] args) throws Exception {
    //
    MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .serverId("56001")
            .hostname("")
            .port(3306)
            .databaseList("")
            .tableList("onedata_dev.")
            .username("")
            .password("")
            .deserializer(new JsonDebeziumDeserializationSchema())
            .build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(3000);
    env.fromSource(mySqlSource,WatermarkStrategy.noWatermarks(),"Mysql Source")
            .setParallelism(4)
            .print()
            .setParallelism(1);
    env.execute();//"Print Mysql Snashot + Binlog"

  }
}
