package io.github.agileluo.spark.sort

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Top3").setMaster("local")
    val sc = new SparkContext(conf)
    
    sc.textFile("src/main/resources/sort/top.txt", 1)
      .map(line => (line.toInt, line))
      .sortByKey(false)
      .take(3)
      .map(_._2)
      .foreach(println(_))
   
  }
}