package com.sparkSql

import java.sql.{Connection, DriverManager, PreparedStatement}

object MySqlUtiles {
  /**
    * 获取数据库连接
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_spark?user=root&password=root")
  }

  /**
    * 释放数据库连接等资源
    *
    * @param connection
    * @param pstmt
    */
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]) {
    println(getConnection())
  }
}
