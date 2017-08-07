package io.github.agileluo.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object transOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("transOperation").setMaster("local")
    val sc = new SparkContext(conf)
    
    sc.parallelize(Array(1, 2, 3, 4, 5), 1).map(n => n * 2).foreach(println(_))
    sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 1).filter( _ % 2 == 0).foreach(println(_))
    
    sc.parallelize(Array("hello you", "hello me", "hell world"), 1).flatMap(_.split(" ")).foreach(println(_))
    
    sc.parallelize(Array(Tuple2("class1", 80), Tuple2("class2", 75),
        Tuple2("class1", 90), Tuple2("class2", 60)), 1)
        .groupByKey()
        .foreach(s => {
          println(s._1);
          s._2.foreach(sc => print(sc + " " ))
          println()
        })
        
     sc.parallelize(Array(Tuple2("class1", 80), Tuple2("class2", 75),
        Tuple2("class1", 90), Tuple2("class2", 60)), 1)
        .reduceByKey( _ + _)
          .foreach(s => {
          println(s._1 + " " + s._2);
        })
        
      sc.parallelize(Array(Tuple2(65, "leo"), Tuple2(50, "tom"), 
        Tuple2(100, "marry"), Tuple2(85, "jack")), 1)
        .sortByKey(false)
        .take(3)
        .foreach(s => println(s._1 + " " + s._2))
            
      
      sc.parallelize(Array(
        Tuple2(1, "leo"),
        Tuple2(2, "jack"),
        Tuple2(3, "tom")), 1).join(
            sc.parallelize(Array(
        Tuple2(1, 100),
        Tuple2(2, 90),
        Tuple2(3, 60)), 1))
        .foreach(s => println(s._1 + ":" + s._2._1 + " " + s._2._2))
  }
}