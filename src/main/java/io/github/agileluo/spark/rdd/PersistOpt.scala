package io.github.agileluo.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PersistOpt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PersistOpt").setMaster("local")
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("D:/spark.txt", 1);
    var startTime = System.currentTimeMillis()
    var count = lines.count()
    println(count)
    println("first time: " + (System.currentTimeMillis() - startTime))
    startTime = System.currentTimeMillis()
    count = lines.count()
    println(count)
    println("second time: " + (System.currentTimeMillis() - startTime))
  }
}