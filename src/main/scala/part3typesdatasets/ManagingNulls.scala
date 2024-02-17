package part3typesdatasets

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import part3typesdatasets.ManagingNulls.colasceExpression

object ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  // TODO : -> select the first non-null value
  /**
   TODO
    For every row in dataframe spark will check the
    Rotten_Tomatoes_Rating first and if it is  null
    then spark will pick this
    IMDB_Rating
   */
  val colasceExpression=coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    colasceExpression.as("Coalesce")
  )

  //TODO : ->  checking for nulls
  val filterNullExpression: Column =col("Rotten_Tomatoes_Rating").isNull

  moviesDF.select("*").where(filterNullExpression)

  //TODO: -> nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  //TODO :-> removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // todo : remove rows containing nulls

  //todo replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
  ).show()
}
