package com.pro;

import com.pro.Pojo.Event;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlJdbcTest {
  public static void main(String[] args) throws SQLException {
    String url="";
    String username="";
    String password = "";
    Connection connection = null;
    Statement statement = null;
    Event event = null;
    try {
      //加载驱动，创建链接
      Class.forName("com.mysql.jdbc.Driver");
      //创建与mysql的连接
      connection = DriverManager.getConnection(url, username, password);
      //获取执行语句
      statement = connection.createStatement();
      //得到结果集
      ResultSet resultSet = statement.executeQuery("SELECT * FROM onedata_dev.eventinfo");
      //
      while (resultSet.next()){
        event = new Event(
                resultSet.getNString("user"),
                resultSet.getString("action"),
                resultSet.getLong("times")
        );
        System.out.println(event);
      }

    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }finally{
      //关闭连接，释放资源
      if(connection!=null){
        connection.close();
      }
      if (statement!=null){
        statement.close();
      }
    }
  }
}
