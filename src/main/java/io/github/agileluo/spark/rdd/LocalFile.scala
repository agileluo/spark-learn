package io.github.agileluo.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author Administrator
 */
object LocalFile {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("LocalFile")
      .setMaster("local");
    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:/spark.txt", 1);
    lines.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .foreach(word => println(word._1 + " " + word._2))
     
     var count =  sc.textFile("D:/article.txt", 1).map(line => line.length()).reduce(_ + _);
     println(count)
  }

}