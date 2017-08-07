package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType


object UDAF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDAF").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    var rdd = sc.parallelize(Array("Leo", "Marry", "Jack", "Tom", "Tom", "Tom", "Leo"), 1)
      .map(name => Row(name))
    
     val structType = StructType(Array(
      StructField("name", StringType, true))
     )
    
    val df = sqlContext.createDataFrame(rdd, structType)
        
    df.registerTempTable("names")
    
    sqlContext.udf.register("stringCount", new StringCount)
    
    
    sqlContext.sql("select name, stringCount(name) from names group by name").show()
    
  }
}