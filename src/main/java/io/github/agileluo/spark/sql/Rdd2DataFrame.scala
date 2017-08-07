package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object Rdd2DataFrame {

  case class Student(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Rdd2DataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val studentDf = sc.textFile("src/main/resources/sql/students.txt", 1)
      .map(line => line.split(","))
      .map(arr => Student(arr(0).trim().toInt, arr(1), arr(2).trim().toInt))
      .toDF()

    studentDf.registerTempTable("students")
    val teenagerRDD = sqlContext.sql("select * from students where age < 18").rdd

    teenagerRDD.map(
      arr => Student(arr(0).toString().toInt, arr(1).toString, arr(2).toString().toInt))
      .collect()
      .foreach(s => println(s.id + " " + s.name + " " + s.age))

    teenagerRDD.map(row => Student(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age")))
      .collect()
      .foreach(s => println(s.id + " " + s.name + " " + s.age))
      
     teenagerRDD.map(row => {
       val map = row.getValuesMap(Array("id", "name", "age"))
       Student(map("id").toString().toInt, map("name").toString(), map("age").toString().toInt)
     })
     .collect()
     .foreach(s => println(s.id + " " + s.name + " " + s.age))
     
  }
}