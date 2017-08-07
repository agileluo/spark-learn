package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode


object GenericLoadSave {
  
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("GenericLoadSave").setMaster("local")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
     
      /*val userDF = sqlContext.read.load("hdfs://spark1:9000/users.parquet")
      userDF.write.mode(SaveMode.Overwrite).save("hdfs://spark1:9000/users_saved.parquet")*/
      
      val personDF = sqlContext.read.format("json").load("hdfs://spark1:9000/person.json")
      personDF.write.format("json").mode(SaveMode.Overwrite).save("hdfs://spark1:9000/person_saved.json")
      
      
  }
}