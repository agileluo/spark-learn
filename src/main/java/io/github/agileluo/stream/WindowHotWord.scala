package io.github.agileluo.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object WindowHotWord {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[2]").setAppName("WindowHotWord")
     val sc = new StreamingContext(conf, Seconds(5))
     
     
     val wordDStream = sc
       .socketTextStream("spark2", 9999)
       .map( line =>  (line.split(" ")(0), 1))
      
     wordDStream
       .reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(30), Seconds(10))
       .transform( rdd => {
         rdd.map{v => (v._2, v._1)}
         .sortByKey(false)
         .take(3)
         .foreach(v => println("top: " + v._2 + " " + v._1) )
         rdd
       }).print()
       
      
     sc.start()
     sc.awaitTermination()
  }
}