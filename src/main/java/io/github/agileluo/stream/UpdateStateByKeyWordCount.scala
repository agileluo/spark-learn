package io.github.agileluo.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object UpdateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWordCount")
     val sc = new StreamingContext(conf, Seconds(5))
     sc.checkpoint("hdfs://spark1:9000/wordcount_checkpoint")
     
     sc.socketTextStream("spark2", 9999)
       .flatMap( _.split(" ") )
       .map((_, 1))
       .updateStateByKey((values: Seq[Int], state: Option[Int]) => {
          var newValue = state.getOrElse(0)
          for(v <- values){
            newValue += v
          }
          Option(newValue)
       })
       .print()
       
     sc.start()
     sc.awaitTermination()
  }
}