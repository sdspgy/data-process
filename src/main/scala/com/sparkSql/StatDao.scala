package com.sparkSql

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

object StatDao {
  def insertTop(list: ListBuffer[Info]): Unit = {

    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySqlUtiles.getConnection()

      connection.setAutoCommit(false) //手动提交

      val sql = "insert into work(name,id,age) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.name)
        pstmt.setLong(2, ele.id)
        pstmt.setLong(3, ele.age)
        pstmt.addBatch()
      }
      pstmt.executeBatch() //执行批量处理
      connection.commit() //手工提交
    }
    catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySqlUtiles.release(connection, pstmt)
    }
  }
}
