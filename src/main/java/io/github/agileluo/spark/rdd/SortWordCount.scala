package io.github.agileluo.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SortWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortWordCount").setMaster("local")
    val sc = new SparkContext(conf)
    
    sc.textFile("D:/spark.txt", 1)
      .flatMap(_.split(" "))
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .map(w => (w._2, w._1))
      .sortByKey(false)
      .take(10)
      .map(w => (w._2, w._1))
      .foreach(w => println(w._1 + " " + w._2 ))
  }
}