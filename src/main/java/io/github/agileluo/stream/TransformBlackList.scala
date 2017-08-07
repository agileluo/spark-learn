package io.github.agileluo.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object TransformBlackList {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[2]").setAppName("TransformBlackList")
     val sc = new StreamingContext(conf, Seconds(5))
     
     val blackRDD = sc.sparkContext.parallelize(Array(("java", true)), 1)
     
     val logDStream =  sc.socketTextStream("spark2", 9999)
       .map(line => (line.split(" ")(1), line))
     
     logDStream.transform(rdd => {
       rdd.leftOuterJoin(blackRDD)
       .filter(!_._2._2.getOrElse(false))
       .map(_._2._1)
     })
     .print()
     
       
     sc.start()
     sc.awaitTermination()
  }
}