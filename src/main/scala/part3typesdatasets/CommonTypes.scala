package part3typesdatasets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

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
  //TODO lit(47) it is also an Expression
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  //TODO : -> Booleans type
  //TODO : here we have chained the the filter expression and that in turn will return Column which is also expression
  val dramaFilter: Column = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter: Column = col("IMDB_Rating") > 7.0
  val preferredFilter: Column = dramaFilter and goodRatingFilter

// TODO : where takes argument of type Expression or Column which is also an Expression
  moviesDF.select("Title").where(dramaFilter)
  // + multiple ways of filtering

  // todo : adding one more column based on Expression
  //  In short we are trying to evaluate the filter Expression as value
  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  // TODO : filter on a boolean column
  //TODO here good_movie is Column or Expression which evaluates to Boolean
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === "true")

  // TODO : negations i.e for false values
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

  //todo : Numbers and math operators
  //todo : Using Mathematical based Expression in select method
  //TODO Heer also we have chained multiple Expressions and made the Single Expression
  //TODO if we want to evaluate the the value from this Expression then we have to use alias method over this expression object
  val mathematicalExpression: Column =  (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), mathematicalExpression.as("AverageOFTomatoRatingAndIMDBRating"))

  //todo: -> correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* TODO :-> corr is an ACTION */)

  // TODO Strings manipulation i.e String handling based Expression

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // TODO : -> capitalization: initcap, lower, upper
  //TODO initCap is also Expression that will capitalise the first letter in the respective column
  //TODO ford_torino -> Ford_Torino
  val inItCapExpression: Column = initcap(col("Name"))

  carsDF.select(inItCapExpression)

  //TODO: ->  contains
  //TODO where works like filter the DF on the base of the given Expression
  //TODO where takes an Expression or Column
  //TODO Expression: -> col("Name").contains("volkswagen")
  val predicateExpression :Column  = col("Name").contains("volkswagen")
  val carProjectDF: Dataset[Row] = carsDF.select("*").where(predicateExpression)

  // TODO : -> regex it is more powerful then contains because it works on regex pattern
  val regexString = "volkswagen|vw"
  val regexBasedExpression: Column = regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  val wherePredicateExpression: Column = col("regex_extract") =!= ""
  val vwDF = carsDF.select(
    col("Name"),
    regexBasedExpression
  ).where(wherePredicateExpression).drop("regex_extract")
 vwDF.show(false)

  //TODO With All Columns
 // val colNames: immutable.Seq[Column]  = carsDF.columns.map(name => col(name)).toList
 //val df = carsDF.select(colNames:_* ,regexBasedExpression ).where(wherePredicateExpression).drop("regex_extract")
 // df.show(false)

  val regexReplaceExpression: Column =regexp_replace(col("Name"), regexString, "People's Car")
  vwDF.select(
    col("Name"),
    regexReplaceExpression.as("regex_replace")
  ).show(false)

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
  val complexRegex: String = getCarNames.map(_.toLowerCase()).mkString("|") // volskwagen|mercedes-benz|ford
  val regexExpression=  regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  carsDF.select(
    col("Name"),
    regexExpression
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")


  // todo version 2 - contains
  //TODO here i have created the List of Filters or chained of Filter Expression
  val carNameFilters: Seq[Column] = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  //Todo Now i need to chained them or Combine them
  //TODO lit(false) is the default value we need to pass fold initial value
  val bigFilter: Column = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show


}
