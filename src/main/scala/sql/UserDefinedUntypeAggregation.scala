package sql

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object UserDefinedUntypeAggregation {

  object MyAverge extends UserDefinedAggregateFunction {
    /**
      * 输入数据
      * 名字只是作为列名占位符。
      *
      * @return
      */
    override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

    /**
      * 用于定义存储聚合运算时产生的中间数据结果的Schema
      *
      * @return
      */
    override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

    /**
      * 标明了UDAF函数的返回值类型
      *
      * @return
      */
    override def dataType: DataType = DoubleType

    /**
      * 用以标记针对给定的一组输入，UDAF是否总是生成相同的结果
      *
      * @return
      */
    override def deterministic: Boolean = true

    /**
      * 就是对聚合运算中间结果的初始化
      * 就是bufferSchema 里的数据进行初始化
      *
      * @param buffer
      */
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L

    }

    /**
      *
      * @param buffer bufferSchema中的两个字段（sum和count）
      * @param input inputSchema对应的字段
      */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(0) + 1


      }
    }

    /**
      * merge函数负责合并两个聚合运算的buffer，再将其存储到MutableAggregationBuffer中：
      * @param buffer1
      * @param buffer2
      */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

    }

    /**
      * 最后，由evaluate函数完成对聚合Buffer值的运算，得到最后的结果：
      * @param buffer
      * @return
      */
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark SQL UDAF").master("local[2]").getOrCreate()

    spark.udf.register("myAverage", MyAverge)

    val df = spark.read.json("file://///Users/guanliyuan/IdeaProjects/sparklearn/src/main/resources/employees.json")
    //    df.show()
    df.printSchema()
    df.createOrReplaceTempView("employees")
    val result = spark.sql("select myAverage(salary) from employees")
    result.show()

  }

}
