package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ParquetPartitionDiscovery {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetPartitionDiscovery").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    val userDF = sqlContext.read.parquet("hdfs://spark1:9000/users/gender=male/country=cn/users.parquet");
    userDF.printSchema()
    userDF.show()
  }
}