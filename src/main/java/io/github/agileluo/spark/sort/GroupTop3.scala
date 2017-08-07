package io.github.agileluo.spark.sort

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Arrays

object GroupTop3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupTop3").setMaster("local")
    val sc = new SparkContext(conf)
    
    sc.textFile("src/main/resources/sort/score.txt", 1)
      .map(line => {
        val p = line.split(" ")
        (p(0), p(1).toInt)
       })
       .groupByKey()
       .map(sc => {
         var arr = sc._2.toArray
         arr = arr.sortWith( _ > _)
         (sc._1, Array(arr(0), arr(1), arr(2)))
       })
       .foreach(w => {
         println(w._1)
         for(s <- w._2){
           print(s + " ")
         }
         println()
       })
  }
}