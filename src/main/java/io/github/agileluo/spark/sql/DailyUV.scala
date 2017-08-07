package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.HashSet
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object DailyUV {
  
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("DailyUV").setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      
       val userAccessLog = Array(
        "2015-10-01,1122",
        "2015-10-01,1122",
        "2015-10-01,1123",
        "2015-10-01,1124",
        "2015-10-01,1124",
        "2015-10-02,1122",
        "2015-10-02,1121",
        "2015-10-02,1123",
        "2015-10-02,1123");
    val userAccessLogRDD = sc.parallelize(userAccessLog, 5)
    
    userAccessLogRDD.map(line => {
      var arr = line.split(",")
      (arr(0), arr(1))
    }).groupByKey().map(r => {
      val list = r._2
      val set = HashSet[String]()
      set ++= list
      (r._1, set.size)
    }).collect().foreach(println(_))
    
    import sqlContext.implicits._
    
    val userAccessLogRowRDD = userAccessLogRDD.map(log => {
      var arr = log.split(",")
        Row(arr(0), arr(1).toInt)
    })
    val structType =  StructType(Array(
          StructField("date", StringType, true),
          StructField("userid", IntegerType, true)))
    val userDF = sqlContext.createDataFrame(userAccessLogRowRDD, structType)
    
    userDF.groupBy("date")
       .agg('date, countDistinct('userid)) 
        .map(row => (row(1).toString(), row(2).toString()))
        .collect()
        .foreach(println(_))
        
        
  }
}