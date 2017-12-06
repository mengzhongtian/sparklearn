package sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object RDDRelation {

  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Demo_HotWords")
      .master("local[2]")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

    import spark.implicits._

    /**
      * 创建DataFrame
      **/

    val DF = spark.createDataFrame((1 to 100).map(x => Record(x, s"value=$x")))
    DF.createOrReplaceTempView("record")
    val sqlDF = spark.sql("select * from record")
    //    sqlDF.collect().foreach(println)
//    sqlDF.foreach(row => {
//      println(row(0), row(1))
//    })

//    sqlDF.foreach(row=>println(row))
//    spark.sparkContext.textFile("").foreach(println)
    val head = spark.sql("select count(*) from record").collect().head.getLong(0)

    val slqDF2 = spark.sql("select key,value from record where key <10")
//    slqDF2.rdd.map(row=>s"key:${row(0)},value:${row(1)}").collect().foreach(println)
//    slqDF2.map(row=>s"key:${row(0)},value:${row(1)}").collect().foreach(println)

    DF.where(" key <10").where($"key"<4).orderBy($"key".desc).select("value").show()

    DF.write.mode(SaveMode.Overwrite).parquet("1.parquet")
    val df3 = spark.read.parquet("1.parquet")

    spark.stop()

  }

}
