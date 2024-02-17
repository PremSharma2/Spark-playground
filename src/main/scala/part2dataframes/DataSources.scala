package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {
  import org.apache.spark.{SparkConf, SparkContext}




  // Now you can work with Hadoop filesystem, etc.

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

val sc= spark.sparkContext
  // Access Hadoop configuration from Spark context
  val hadoopConf = sc.hadoopConfiguration

  // Set Hadoop-related properties, e.g., pointing to your `winutils.exe`
  hadoopConf.set("hadoop.home.dir", "C:\\tools\\hadoop")
  hadoopConf.set("hadoop.bin.path", "C:\\tools\\hadoop\\bin")

  //TODO : Cars Schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
 TODO
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
    .option("mode", "failFast") spark will throw an exception if any record of df is corrupt or malformed
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema while reading the DF
    .option("mode", "failFast") // dropMalformed, permissive (default mode )
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(
      Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
 TODO
   Writing DFs:
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
  */
     carsDF
    .write
    .format("json")
    .mode(SaveMode.Overwrite) // overwrite the earlier snapshot with this new one
    .save("src/main/resources/data/cars_dupe.json")


  // JSON flags
    spark
      .read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")


  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet default format for DFs
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy Parquet
    * - table "public.movies" in the Postgres DB
    */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // TODO :TSV format
  //moviesDF.write
  //  .format("csv")
  //  .option("header", "true")
   // .option("sep", "\t")
  //  .save("src/main/resources/data/movies.csv")

  // Parquet
 // moviesDF.write.save("src/main/resources/data/movies.parquet")

  // save to DF
  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
