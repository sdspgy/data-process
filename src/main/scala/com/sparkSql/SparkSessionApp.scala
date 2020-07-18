package com.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

object DataFrameRDDApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    //RDD ==> DataFrame
    val rdd = spark.sparkContext.textFile("file:///Users/hoolai/Documents/tyz/zhiqiu/data/text.txt")

    //反射 case class
    inferRef(spark, rdd)
    //编程 Row
    //program(spark, rdd)

    //操作mysql数据库
    //    exctMysql(spark)
    spark.stop()
  }

  private def inferRef(spark: SparkSession, rdd: RDD[String]) = {
    import spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1).toInt, line(2))).toDF()
    infoDF.show()
    infoDF.select(infoDF.col("name")).show()

    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 1").show()
    //dataset 编译类型安全
    val ds = infoDF.as[Info]
    ds.map(line => line.id).show
    ds.map(line => line.id).write.format("json").save("file:///Users/hoolai/Documents/tyz/zhiqiu/data/writeJson.json")
    // 写入mysql数据库
    infoDF.foreachPartition(partitionOfRecords => {
      val list = new ListBuffer[Info]

      partitionOfRecords.foreach(info => {
        val id = info.getAs[Int]("id")
        val age = info.getAs[Int]("age")
        val name = info.getAs[String]("name")

        list.append(Info(id, age, name))
      })
      StatDao.insertTop(list)
    })
  }

  private def program(spark: SparkSession, rdd: RDD[String]) = {
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1).toInt, line(2)))
    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("age", IntegerType, true),
      StructField("name", StringType, true)))
    val infoDF = spark.createDataFrame(infoRDD, structType)
    infoDF.printSchema()
    infoDF.show()
    infoDF.select(infoDF.col("name")).show()

    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 1").show()
  }

  private def exctMysql(spark: SparkSession) = {
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/hivedb")
      .option("dbtable", "hivedb.TBLS")
      .option("user", "root")
      .option("password", "root")
      .load()
    jdbcDF.printSchema()
    jdbcDF.show()
  }

}
