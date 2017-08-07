package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import scala.collection.Map

object JDBCDataSource {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    
    val accountDF = sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://10.120.54.10:3306/oauth2?useUnicode=true&characterEncoding=UTF-8",
          "user" -> "admin",
          "password" -> "123456*",
          "dbtable" -> "user_account"
          ))
      .load()
    
    val accountUrlDF= sqlContext.read.format("jdbc")
      .options(Map("url" -> "jdbc:mysql://10.120.54.10:3306/oauth2?useUnicode=true&characterEncoding=UTF-8",
          "user" -> "admin",
          "password" -> "123456*",
          "dbtable" -> "user_account_url"
          ))
      .load()
      
     accountDF.rdd.map (r => (r.getAs[Long]("id"), r.getAs[String]("userId") ))
       .join(accountUrlDF.rdd.map( r => (r.getAs[Long]("userAccountId"), r.getAs[String]("url") )))
       .groupByKey()
       .map(r => {
           val id = r._1
           val list = r._2
           var userId = list.last._1
           var count = 0
           for(e <- list) count +=1
           (count, userId)
       })
       .sortByKey(false)
       .take(5)
       .foreach(r => println(r._2 + " " + r._1))
  }
}