package io.github.agileluo.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ActionOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ActionOperation").setMaster("local")
    val sc = new SparkContext(conf)
    
    println("sum: " + sc.parallelize(Array(1, 2, 3, 4, 5), 1).reduce(_ + _))
    
    val collectArray = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 1).map(_ * 2).collect();
    for( n <- collectArray) {println(n)}
    
    println("count: " + sc.parallelize(Array(1, 2, 3, 4, 5), 1).count())
    
    val top3 = sc.parallelize(Array(1, 2, 5, 9, 7, 4), 1).take(3)
    for(n <- top3) {println(n)}
    
    println("keyCount: " + sc.parallelize(Array(Tuple2("class1", "leo"), Tuple2("class2", "jack"),
        Tuple2("class1", "tom"), Tuple2("class2", "jen"), Tuple2("class2", "marry")), 1)
        .countByKey()    
    )
  }
}