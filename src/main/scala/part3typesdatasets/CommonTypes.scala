package part3typesdatasets

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // TODO : -> adding a plain value to a DF
  //TODO this lit function works regardless what kind of values we use i.e we an use with any type
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  //TODO : -> Booleans type
  //TODO : here we have chained the the filter expression and that in turn will return Column which is also expression
  val dramaFilter: Column = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter: Column = col("IMDB_Rating") > 7.0
  val preferredFilter: Column = dramaFilter and goodRatingFilter

// TODO : where takes argument of type Expression or Column which is also an Expression
  moviesDF.select("Title").where(dramaFilter)
  // + multiple ways of filtering

  // todo : adding one more column based on Expression in short we are trying to evaluate the filter expression as value
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  // TODO : filter on a boolean column
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === "true")

  // TODO : negations i.e for false values
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

  //todo : Numbers and math operators
  //todo : Mathematical based Expression in select method
  val mathematicalExpression: Column =  (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), mathematicalExpression.as("AverageOFTomatoRatingAndIMDBRating"))

  //todo: -> correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* TODO :-> corr is an ACTION */)

  // TODO Strings manipulation i.e String handling based Expression

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // TODO : -> capitalization: initcap, lower, upper
  //TODO initCap will capatilise the first letter in the respective column
  //TODO ford_torino -> Ford_Torino
  carsDF.select(initcap(col("Name")))

  //TODO: ->  contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // TODO : -> regex it is more powerful then contains because it works on regex pattern
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  /**
    * Exercise
    *
    * Filter the cars DF by a list of car names obtained by an API call
    * Versions:
    *   - contains
    *   - regexes
    */

  def getCarNames: List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // todo  1 - regex
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|") // volskwagen|mercedes-benz|ford
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")

  // todo version 2 - contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show


}
