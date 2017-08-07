package io.github.agileluo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType


object RowNumberTop3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RowNumberTop3").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    
    import sqlContext.implicits._
    
    var rdd = sc.textFile("src/main/resources/sql/sales.txt", 1)
       .map(line => { 
         var arr = line.split('\01'); 
         Row(arr(0), arr(1), arr(2).toInt)  
       })
    
     val structType = StructType(Array(
      StructField("product", StringType, true),
      StructField("category", StringType, true),
      StructField("revenue", IntegerType, true))
      )
    
    val top3DF = sqlContext.createDataFrame(rdd, structType)
        
    top3DF.registerTempTable("sales")
    
    var resultDF =  sqlContext.sql("select product, category, revenue "
        + "from ( select product, category, revenue, "
        +" row_number() over (partition by category order by revenue desc) rank from sales ) temp_sales"
        + " where rank <= 3 "    
    )
      
    resultDF.show()
  }
}