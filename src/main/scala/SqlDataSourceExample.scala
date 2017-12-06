import org.apache.spark.sql.{SaveMode, SparkSession}

object SqlDataSourceExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Demo_HotWords")
      .master("local[2]")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()
    //    runBasicDataSourceExample(spark)

    //    runBasicParquetExample(spark)

    //    runParquetSchemaMergingExample(spark)
    runJdbcDatasetExample(spark)
  }


  private def runBasicDataSourceExample(spark: SparkSession): Unit = {
    //        val userDF = spark.read.parquet("file:////C:\\IdeaRepository\\sparklearn\\src\\main\\resources/users.parquet")
    //        userDF.select("name","favorite_color").write.parquet("nameAndfavorite_color.parquet")

    val peopleDF = spark.read.json("file:////C:\\IdeaRepository\\sparklearn\\src\\main\\resources/people.json")
    //        spark.read.format("json").load()

    /*val csvDF = spark.read.csv("file:////C:\\IdeaRepository\\sparklearn\\src\\main\\resources/people.csv")
    csvDF.printSchema()
    csvDF.show()
    */

    /**
      * 从csv读取
      */
    /*    spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", false)
      .load("file:////C:\\IdeaRepository\\sparklearn\\src\\main\\resources/people.csv")
      .show()*/

    /**
      * sql语句中直接读取
      */
    /*   spark.sql("select * from parquet.`file:////C:\\\\IdeaRepository\\\\sparklearn\\\\src\\\\main\\\\resources/users.parquet`")
         .show()*/

    /**
      *
      */
    //    peopleDF.write.bucketBy(42,"name").saveAsTable("people_bucketed")

    //    userDF.write.partitionBy("favorite_color").parquet("Myparquet")

    spark.sql("Drop table if exists people_bucketed")

  }

  private def runBasicParquetExample(spark: SparkSession) = {
    import spark.implicits._
    val peopleDF = spark.read.json("file:////C:\\IdeaRepository\\sparklearn\\src\\main\\resources/people.json")
    peopleDF.write.parquet("people.parquet")
    val parquetDF = spark.read.parquet("people.parquet")
    parquetDF.createOrReplaceTempView("parquetPeople")
    val namesDF = spark.sql("select name from parquetPeople where age between 13 and 19")
    namesDF.map(x => "name:" + x.get(0)).show()
  }

  private def runParquetSchemaMergingExample(spark: SparkSession): Unit = {
    import spark.implicits._
    val DF = spark.sparkContext.makeRDD(1 to 5).map(num => (num, "mike_" + num)).toDF("a", "b")
    val DF2 = spark.sparkContext.makeRDD(6 to 10).map(num => (num, "mike_" + num)).toDF("a", "c")
    DF.write.parquet("data/key=1")
    DF2.write.parquet("data/key=2")
    val resultDF = spark.read.option("mergeSchema", true).parquet("data")
    resultDF.printSchema()
    resultDF.show()

    /**
      * +---+------+-------+---+
      * |  a|     b|      c|key|
      * +---+------+-------+---+
      * |  8|  null| mike_8|  2|
      * |  9|  null| mike_9|  2|
      * | 10|  null|mike_10|  2|
      * |  3|mike_3|   null|  1|
      * |  4|mike_4|   null|  1|
      * |  5|mike_5|   null|  1|
      * |  6|  null| mike_6|  2|
      * |  7|  null| mike_7|  2|
      * |  1|mike_1|   null|  1|
      * |  2|mike_2|   null|  1|
      * +---+------+-------+---+
      */

  }

  private def runJsonDatasetExample(spark: SparkSession) = {
    import spark.implicits._
    val ds = spark.createDataset("""{"name":"mike","age":18}""" :: Nil)
    spark.read.json(ds)

  }

  private def runJdbcDatasetExample(spark: SparkSession) = {
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://10.6.5.251:3306")
      .option("dbtable", "recdb.hot_words")
      .option("user", "root")
      .option("password", "cdv123098")
      .load()

    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://10.6.5.251:3306")
      .option("dbtable", "mike.hot_words")
      .option("user", "root")
      .option("password", "cdv123098")
      .option("SaveMode", "")
      .mode(SaveMode.Append)
      .save()

    jdbcDF.show()

  }


}
