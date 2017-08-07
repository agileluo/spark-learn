package io.github.agileluo.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ParallelizeCount {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("ParallelizeCount").setMaster("local");
    val sc = new SparkContext(conf);
    
    val sum = sc.parallelize(Array(1,2,3,4,5), 1).reduce(_ + _);
    println(sum)
    
  }
}