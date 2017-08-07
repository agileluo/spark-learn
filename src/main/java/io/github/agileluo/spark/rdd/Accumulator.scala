package io.github.agileluo.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sc = new SparkContext(conf)
    
    val ac = sc.accumulator(0)
    
    sc.parallelize(Array(1, 2, 3, 4, 5), 1).foreach( ac += _)
    
    println(ac)
  }
}