import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Demo_HotWords")
      .master("local[2]")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    spark.read.parquet("1.parquet").show()
  }

}
