import org.apache.spark.sql.SparkSession

object JoinTest {

  case class Person(age: String, name: String)

  case class Person2(age2: String, name: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Demo_HotWords")
      .master("local[2]")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

    import spark.implicits._
    val df = spark.sparkContext.textFile("1.txt").map(_.split(" ")).map(x => Person(x(0), x(1))).toDF()
    val df2 = spark.sparkContext.textFile("2.txt").map(_.split(" ")).map(x => Person(x(0), x(1))).toDF()
    df.join(df2, "age").show(false)



  }

}
