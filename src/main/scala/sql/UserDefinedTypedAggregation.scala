package sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator


object UserDefinedTypedAggregation {

  case class Employee(name: String, salary: Long)

  case class Average(var sum: Long, var count: Long)

  /**
    * [Employee, Average, Double]
    *
    * IN The input type for the aggregation.
    * BUF The type of the intermediate value of the reduction.
    * OUT The type of the final output result.
    *
    */
  object MyAverage extends Aggregator[Employee, Average, Double] {
    /**
      * 初始值
      * @return 返回中间结果 BUF
      */
    override def zero: Average = Average(0L, 0L)

    /**
      * 聚合运算中，每次进来一个数据进行处理
      * @param b 中间结果 BUF
      * @param a 新进来的数据
      * @return 返回的数据
      */
    override def reduce(b: Average, a: Employee): Average = {
      b.sum += a.salary
      b.count += 1
      b

    }

    /**
      * 中间缓存进行合并的处理
      * @param b1
      * @param b2
      * @return
      */
    override def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count+=b2.count
      b1
    }


    /**
      * 返回最终的结果
      * @param reduction
      * @return
      */
    override def finish(reduction: Average): Double = {
      reduction.sum.toDouble/reduction.count
    }

    /**
      * 编码，序列化方式
      * @return
      */
    override def bufferEncoder: Encoder[Average] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark").master("local[2]").getOrCreate()
    import spark.implicits._

    val ds=spark.read.json("file://///Users/guanliyuan/IdeaProjects/sparklearn/src/main/resources/employees.json")
          .as[Employee]

    ds.show()

    /**
      * 返回类型是：TypedColumn
      * ds.select()里传入的类型就是TypedColumn
      */
    val averageSalary=MyAverage.toColumn.name("average_salary")
    val result=ds.select(averageSalary)
    result.show()




  }


}
