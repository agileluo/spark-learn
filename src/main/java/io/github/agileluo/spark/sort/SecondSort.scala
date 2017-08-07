package io.github.agileluo.spark.sort

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SecondSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondSort").setMaster("local")
    val sc = new SparkContext(conf)
    
    sc.textFile("src/main/resources/sort.txt", 1)
      .map(line => {
        var p = line.split(" ")
        (new SecondSortKey(p(0).toInt, p(1).toInt), line)
      })
      .sortByKey(false)
      .map(_._2)
      .foreach(println(_))
    
  }
}