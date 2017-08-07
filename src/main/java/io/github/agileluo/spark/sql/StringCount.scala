package io.github.agileluo.spark.sql

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import  org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

class StringCount extends UserDefinedAggregateFunction {
  def bufferSchema: StructType = {
    new StructType().add("count", IntegerType)
  }
  def dataType: DataType = {
    IntegerType
  }
  def deterministic: Boolean = {
    true
  }
  def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
  def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0
  }
  def inputSchema: StructType = {
    new StructType().add("str", StringType)
  }
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) +   buffer2.getAs[Int](0)
  }
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

}