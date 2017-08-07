package io.github.agileluo.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object HdfsFile {
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("HdsfFile").setMaster("spark://spark1:7077")
    var sc = new SparkContext(conf)
    val count = sc
      .textFile("hdfs://spark1:9000/spark.txt", 1)
      .map(word => word.length())
      .reduce(_ + _)
    println("spark file length " + count)
    sc.stop()
  }
}