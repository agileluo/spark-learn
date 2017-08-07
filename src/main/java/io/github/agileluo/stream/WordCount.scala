package io.github.agileluo.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object WordCount {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
     val sc = new StreamingContext(conf, Seconds(5))
     
     sc.socketTextStream("spark2", 9999)
       .flatMap( _.split(" ") )
       .map((_, 1))
       .reduceByKey(_ + _)
       .print()
       
     sc.start()
     sc.awaitTermination()
  }
}