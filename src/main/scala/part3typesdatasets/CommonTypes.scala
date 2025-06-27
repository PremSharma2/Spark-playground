package part3typesdatasets

import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import spark.dataframe.DataQualityCheck.columns

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
  //TODO lit(47) it is also an Expression but its a Leaf Expression
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  //TODO : -> Booleans type
  //TODO : here we have chained the the filter expression
  // and that in turn will return Column which is also expression
  val dramaFilterExpression: Column = col("Major_Genre") equalTo "Drama"
  val ratingFilterExpression: Column = col("IMDB_Rating") > 7.0
  val chainedFilterExpression: Column = dramaFilterExpression and ratingFilterExpression

// TODO : where takes argument of type Expression or Column which is also an Expression
  moviesDF.select("Title").where(dramaFilterExpression)
  // + multiple ways of filtering

  // todo : adding one more column based on Filter-Expression
  //  In short we are trying to evaluate the filter Expression as value
  //this means that this Expression is
  // attached to this Column preferredFilter.as("good_movie") named good_movie
  val moviesWithGoodnessFlagsDF = moviesDF.select(
                                                    col("Title"),
                                                    chainedFilterExpression.as("good_movie"))

  // TODO : filter on a boolean column
  //TODO here good_movie is Column or Expression which evaluates to Boolean
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === "true")

  // TODO : negations i.e for false values
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))


  //todo : Numbers and math operators
  //todo : Using Mathematical based Expression in select method
  //TODO Heer also we have chained multiple Expressions and made the Single Expression
  //TODO if we want to evaluate the the value from
  // this Expression then we have to use alias
  // method over this expression object
  val mathematicalExpression: Column =  (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2
  val moviesAvgRatingsDF = moviesDF.select(
                                              col("Title"),
                                                mathematicalExpression.as("AverageOFTomatoRatingAndIMDBRating"))

  //todo: -> correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* TODO :-> corr is an ACTION */)


  // TODO Strings manipulation i.e String handling based Expression

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // TODO : -> capitalization: initcap, lower, upper
  //TODO initCap is also Expression
  // that will capitalise the first letter in the respective column
  //TODO ford_torino -> Ford_Torino
  val inItCapExpression: Column = initcap(col("Name")).as("inItCap")

  carsDF.select(inItCapExpression)

  //TODO: ->  contains
  //TODO where works like filter the DF on the base of the given Expression
  //TODO where takes an Expression or Column
  //TODO Expression: -> col("Name").contains("volkswagen")
  //select * from cars where cars.name='wolsvagen'
  val predicateExpression :Column  = col("Name").contains("volkswagen")
  val carProjectDF: Dataset[Row] = carsDF.select("*").where(predicateExpression)

  // TODO : -> regex it is more powerful then
  //  contains because it works on regex pattern

  val regexString = "volkswagen|vw"
  val regexBasedExpression: Column = regexp_extract(col("Name"), regexString, 0)
  val wherePredicateExpression: Column = col("regex_extract") =!= ""

  val vwDF = carsDF.select(
    col("Name"),
    regexBasedExpression.as("regex_extract")
  ).where(wherePredicateExpression)
    .drop("regex_extract")
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

  val regexExpression=  regexp_extract(col("Name"), complexRegex, 0)
  carsDF.select(
    col("Name"),
    regexExpression.as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")


  // todo version 2 - contains
  //TODO here i have created the List of Filters or chained of Filter Expression
  val carNameFilters: Seq[Column] =  getCarNames
                                    .map(_.toLowerCase())
                                    .map(name => col("Name")
                                      .contains(name))
  //Todo Now i need to chained them or Combine them
  //TODO lit(false) is the default value we need to pass fold initial value
  //lit(false) is the neutral identity for the OR operation — it's safe:
  //false OR X → always returns X.
  //this will become like this the combined Expression
  // Column(Or(Or(Literal(false), IsNull(Name)), IsNull(Horsepower)))
  /*
   Spark will evaluate this at runtime like:


  (row: Row) =>
  row.isNullAt("Name") ||
  row.isNullAt("Horsepower") ||
  row.isNullAt("Weight_in_lbs")
  But in logical plan, this is just an Or(Or(...)) expression tree.



   */
  val bigFilter: Column = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show

   /**
      Data Quality check algo
    */

   val columns=carsDF.columns

 // carsDF.select(chainedExpression).show(false)
 val columnstoBeSelected: Array[Column] =columns.map(
   column=> count(when(col(column).isNull,column)).as(column))


  carsDF.select(columnstoBeSelected:_*).show(false)

  /**
   *  Build a chained when expression that gives
   *  each movie a score based on a combination of ratings.
   */
  // Dynamic movie score logic of if and else kind of logic
    //this is called if-else Expression
  val scoreExpr: Column =
      when(col("IMDB_Rating") >= 8.5 && col("Rotten_Tomatoes_Rating") >= 85, 10).
      when(col("IMDB_Rating") >= 7.0 && col("Rotten_Tomatoes_Rating") >= 70, 8).
      when(col("IMDB_Rating") >= 6.0 || col("Rotten_Tomatoes_Rating") >= 60, 5).
      otherwise(2)

  moviesDF
    .select(
      col("Title"),
      col("IMDB_Rating"),
      col("Rotten_Tomatoes_Rating"),
      scoreExpr.alias("movie_score")
    )
    .orderBy(desc("movie_score"))
    .show(false)


  /**
   * Fold over column list to create Composed Expression
   * to check for nulls
   */
  val carNameFilters1: Seq[Column] =
    getCarNames
    .map(_.toLowerCase())
    .map(name => col(name).isNull)

  val anyNullCheck: Column  =
    carNameFilters1.
    fold(lit(false)){
      (combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter
    }


  carsDF
    .withColumn("has_nulls", anyNullCheck)
    .select(col("Name"), col("has_nulls"))
    .filter(col("has_nulls"))
    .show(false)

  /**
   *   Mathematical Expression:->
   *   Generate custom completeness score per row each column (percentage of non-nulls)
   *
   */

  val totalCols = columns.length.toDouble
  val seqOfExpression: Array[Column] = columns.map(colName => when(col(colName).isNull, 1).otherwise(0))
  val nullCountExpr = seqOfExpression.reduce(_ + _)

  val completenessExpr = (lit(totalCols) - nullCountExpr) / lit(totalCols)

  carsDF
    .select(col("Name"), completenessExpr.alias("completeness_score"))
    .orderBy(desc("completeness_score"))
    .show(false)

}
