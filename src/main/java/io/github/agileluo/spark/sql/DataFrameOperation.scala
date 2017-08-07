package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object DataFrameOperation {
  
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("DataFrameOperation").setMaster("local")
      val sc = new SparkContext(conf)
      val sql = new SQLContext(sc)
      val df = sql.read.json("hdfs://spark1:9000/students.json")
      df.show()
      df.printSchema()
      df.select("name").show()
      df.select(df("name"), df("age")+1).show()
      df.select("id", "name", "age").filter(df("age") > 18).show()
      df.groupBy("age").count().show()
  }
}