import com.sun.prism.PixelFormat.DataType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

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
//    df.join(df2, "age").show(false)



    val schemaString ="age name"
    val fields   = schemaString .split(" ").map(x=>StructField(x,DataTypes.StringType,true))
    val schema =StructType(fields)
    val rowRDD = spark.sparkContext.textFile("2.txt").map(_.split(" ")).map(x=>Row(x(0),x(1)))
    spark.createDataFrame(rowRDD,schema )
    
//    val intToString = Map(1->"a1",2->"a2",3->"a3")
      val frame = Seq(Person("1","a1"),Person("2","a2")).toDF()
      frame.printSchema()
      frame.show()



  }

}
