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
import org.apache.spark.sql.types.DoubleType

object DailySale {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DailySale").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._

    val userSaleLog = Array("2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123")
    val userSaleLogRDD = sc.parallelize(userSaleLog, 5)

    val userSaleLogRowRDD = userSaleLogRDD.filter(log => if (log.split(",").length == 3) true; else false)
      .map(log => {
        val arr = log.split(",")
        Row(arr(0), arr(1).toDouble)
      })

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("amount", DoubleType, true))
      )
    
    val saleDF = sqlContext.createDataFrame(userSaleLogRowRDD, structType)
    
    saleDF.groupBy("date")
    .agg('date, sum('amount))
    .map(row => (row(1).toString(), row(2).toString()))
    .collect()
    .foreach(println(_))

  }
}