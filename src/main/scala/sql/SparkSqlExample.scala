package sql

import org.apache.spark.sql.{Encoders, Row, SparkSession}

case class Person(name: String, age: Int)

object SparkSqlLearn {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Demo_HotWords")
      .master("local[2]")
      .getOrCreate()

        runBasicDataFrameExample(spark)


    //    runDatasetCreationExample(spark)


    //    runInferSchemaExample(spark)
//    runProgrammaticSchemaExample(spark)


  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    val df = spark.read.json("file:////C:/mydevelop/spark/examples/src/main/resources/people.json")
    import spark.implicits._
    df.printSchema()
    df.select("name").show()
    df.select($"age").show()

    df.select($"age" === 30 as "age30").show()
    df.groupBy("name").count().show()
    df.groupBy("name").avg("age").show()

    df.createOrReplaceTempView("people")
    val df2 = spark.sql("select * from people ").show()
    val spark2 = spark.newSession()

    df.createGlobalTempView("people2")
    val df3 = spark.sql("select * from global_temp.people2")
    println("df3")
    df3.show()
    println("mmmmmmmmmmmmmmmmmmmmmm")
    df.select("name age")


  }

  private def runDatasetCreationExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = Seq(Person("mike", 17), Person("java", 12)).toDF()
    df.show
    val df2 = List(1, 2, 3).toDF()
    df2.show


    print("--------------")
    val df3 = Seq(1, 23, 3).toDF().map(r => {
      val value = r.get(0)

      val string = value.toString
      string

    }).show()

    print("2222222222222222222222")
    val ds2 = Seq(1, 2, 3, 4).toDS()
    val ints = ds2.map(_ + 1).collect()
    println(ints)
    println("333333333333333333333333333333")
    val df5 = spark.read.json("file:////C:/mydevelop/spark/examples/src/main/resources/people.json")
    val ds5 = df5.as[Person]
    df5.show()

  }

  private def runInferSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    /**
      * RDD -> DF
      */
    val df = spark.read.text("file:////C:/mydevelop/spark/examples/src/main/resources/people.txt")
    //    df.printSchema()
    //    df.show()

    val peopleDF = spark.sparkContext
      .textFile("file:////C:/mydevelop/spark/examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(a => Person(a(0), a(1).trim.toInt))
        .toDF()

    peopleDF.createOrReplaceTempView("people")
    print("----------------------------")
    spark.sql("select * from people where age>=13 and age<=20").show()

    peopleDF.printSchema()
    implicit val anyEncoder = Encoders.kryo[Any]
    peopleDF.map(x => x(0)).show()
    peopleDF.map(row => {
      row.getAs[String]("name")
    }).show()


    implicit val mapEncoder = Encoders.kryo[Map[String, Any]]
    val dff = peopleDF.map(row => {
      row.getValuesMap[Any](List("name", "age"))
      //返回类型：Map[String,Any]
      //在这里，返回值是：Map("name" -> "Justin", "age" -> 19)
    }).collect()
    dff.foreach(println)

  }


  private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    import spark.implicits._
    // $example on:programmatic_schema$
    // Create an RDD
    val peopleRDD = spark.sparkContext.textFile("file:////C:/mydevelop/spark/examples/src/main/resources/people.txt")
    import org.apache.spark.sql.types._
    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val name=StructField("name",DataTypes.StringType,true)
    val age=StructField("age",DataTypes.IntegerType,true)
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, DataTypes.StringType, true))
    val schema = StructType(List(name,age))
    val rowRDD = peopleRDD.map(_.split(","))
      .map(list => Row(list(0), list(1).trim.toInt))

    val peopleDF = spark.createDataFrame(rowRDD, schema)
    peopleDF.show()


    val peopleRDDs = spark.sparkContext
      .textFile("file:////C:/mydevelop/spark/examples/src/main/resources/people.txt")
    peopleRDDs.toDF()

  }


}
