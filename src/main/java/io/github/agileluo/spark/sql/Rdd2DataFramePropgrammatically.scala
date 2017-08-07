package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

object Rdd2DataFramePropgrammatically {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Rdd2DataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val studentRDD = sc.textFile("src/main/resources/sql/students.txt", 1)
      .map(line => line.split(","))
      .map( line => Row(line(0).toInt, line(1), line(2).toInt))
    
    val structType = StructType(Array(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)))
    
    val studentDF = sqlContext.createDataFrame(studentRDD, structType)        
        
    studentDF.registerTempTable("students")
    
    sqlContext.sql("select * from students where age <= 18").show()

     studentDF.rdd.foreach(println(_))
  }
}