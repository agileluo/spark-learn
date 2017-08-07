package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ParquetLoadData {

  case class Student(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetLoadData").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    var userDF = sqlContext.read.parquet("hdfs://spark1:9000/users.parquet")
    userDF.registerTempTable("users")
    val userNameDF = sqlContext.sql("select name from users")
    
    userNameDF.rdd.collect().foreach(println(_))
     
  }
}