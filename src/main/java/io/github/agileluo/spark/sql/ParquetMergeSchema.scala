package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

object ParquetMergeSchema {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GenericLoadSave").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    val studentsWithNameAge = Array(("leo", 23), ("jack", 25)).toSeq
    val studentsWithNameAgeDF = sc.parallelize(studentsWithNameAge, 2).toDF("name", "age")
    studentsWithNameAgeDF.write.format("parquet").mode(SaveMode.Append).save("hdfs://spark1:9000/spark-study/students")

    val studentsWithNameGrade = Array(("marry", "A"), ("tom", "B")).toSeq
    val studentsWithNameGradeDF = sc.parallelize(studentsWithNameGrade, 2).toDF("name", "grade")
    studentsWithNameGradeDF.write.format("parquet").mode(SaveMode.Append).save("hdfs://spark1:9000/spark-study/students")
   
    val students = sqlContext.read.option("mergeSchema", "true")
      .parquet("hdfs://spark1:9000/spark-study/students")
    students.printSchema()
    students.show()
  }
}