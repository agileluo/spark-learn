package io.github.agileluo.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Broadcast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Broadcast").setMaster("local")
    val sc = new SparkContext(conf)
    
    val broadcastVal = sc.broadcast(3);
    sc.parallelize(Array(1, 2, 3, 4, 5), 1).map( _ * broadcastVal.value).foreach(println(_))
            
  }
}