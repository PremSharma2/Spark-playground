package part2dataframes

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, column, expr, when}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn: Column = carsDF.col("Name")

  // selecting (projecting)
  //TODO : We are projecting the existing dataframe into new data frame which has less data
  //TODO Hence Select is also the transformation over DF
  // which takes an Expression to get evaluated by DAG scheduler
  //todo technically we are projecting existing DF int new Df which has less Data
  val carNamesDF: DataFrame = carsDF.select(firstColumn)


  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column object
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") //  TODO :EXPRESSION -> it will also transform this expression into Column object
  )

  //   TODO : -> select with plain column names
  //TODO def select(col: String, cols: String*): DataFrame = select((col +: cols).map(Column(_)) : _*)
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression: Column = carsDF.col("Weight_in_lbs")
  val weightInKgExpression: Column = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  //TODO : alternatively we can use  selectExpr
//TODO use this when we want to evaluate multiple Expression Like this
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  //TODO:-> DF processing

  // adding a column
  val carsWithKg3DF: DataFrame = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val filterColumnExpression: Column = col("Origin") =!= "USA"

  val europeanCarsDF = carsDF.filter(filterColumnExpression)

  val europeanCarsDF2 = carsDF.where(filterColumnExpression)

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  //TODO:-> chain the  filter Expressions i.e
  // will chain two Column object expression and return single Column object which is an Expression
  val chainedFilterExpression= col("Origin") === "USA" and col("Horsepower") > 150
  val chainedFilterExpression1= col("Origin") === "USA" and col("Origin") contains("US")
  //TODO when expression
  val chainedAndExpression: Column = col("Major_Genre") contains("Comedy") and col("IMDB_Rating") > 6
  val chainedAndExpression1: Column = col("Major_Genre") contains("Comedy") and col("IMDB_Rating") < 6

  val result = moviesDF
         .withColumn("priority",
              when(chainedAndExpression, "High")
              .when(chainedAndExpression1, "Low")
             .otherwise("Normal"))


  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(chainedFilterExpression)
  // filtering with expression strings
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  //overlaying the Adjustment with Snapshot
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()

  /**
    * Exercises
    *
    * 1. Read the movies DF and select 2 columns of your choice
    * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
    * 3. Select all COMEDY movies with IMDB rating above 6
    *
    * Use as many versions as possible
    */

  val moviesDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  moviesDF.show()

  // 1
  val moviesReleaseDF = moviesDF.select("Title", "Release_Date")
  val moviesReleaseDF2 = moviesDF.select(
    moviesDF.col("Title"),
    col("Release_Date"),
    $"Major_Genre",
    expr("IMDB_Rating")
  )
  val moviesReleaseDF3 = moviesDF.selectExpr(
    "Title", "Release_Date"
  )

  // 2
  val moviesProfitDF = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross")
  )

  val moviesProfitDF2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  val moviesProfitDF3 = moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

  // 3
  //TODO this is an expression here we have chained the filter
  // Expression = col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6
  val chainedExpression: Column =col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6



  val atLeastMediocreComediesDF = moviesDF.select("Title", "IMDB_Rating")
    .where(chainedExpression)

  val comediesDF2 = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  val comediesDF3 = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  comediesDF3.show
}
