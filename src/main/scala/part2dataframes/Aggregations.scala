package part2dataframes

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  // counting the col value
    val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)")

  // counting all
  moviesDF.select(count("*")) // count all the rows, and will INCLUDE nulls

  // counting distinct values of Major_Genre across all movies
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  // TODO :approximate count helpful in data analytics
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min and max
  val minRatingDF: DataFrame = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")).as("Total-Gross"))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  // Grouping

  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count()  // select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

//TODO It is good when we want to use two aggregate function
  val aggregationsByGenreDF = moviesDF
      .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

 /**
     TODO
      Conditional Aggregation:
 TODO
       Conditional aggregation with `max(when(...))` in Spark and Scala is useful for performing
      complex aggregations based on specific conditions within a group.
      This technique allows you to apply conditions to rows
      within each group before the aggregation function (like `max`) is executed.
      It's particularly beneficial when you need to compute aggregates
      over subsets of data that meet certain criteria, without having to filter the dataset beforehand.
   TODO
      In the  provided code example,
      we use conditional aggregation to find the maximum production budget
      for each genre, but only considering movies released after the year 2000. Here's a breakdown:
 TODO
   - `groupBy("Major_Genre")`: Groups the data by the `Major_Genre` column.
   - `agg(...)`: Performs aggregation on the grouped data.
   - `max(when($"Release_Year" > 2000, $"Production_Budget"))`:
     This expression uses `when` to apply a condition within each group.
     It selects the `Production_Budget` values only for those rows
     where `Release_Year` is greater than 2000.
     Then, `max` calculates the maximum of these selected values for each genre group.
   - `.alias("Max_Production_Budget_Post_2000")`:
     Renames the result of the aggregation to `Max_Production_Budget_Post_2000`.

This approach is beneficial when analyzing trends or extracting insights from data subsets without splitting the dataset into multiple parts. It simplifies the process of computing conditional statistics across different segments of your data, making your data analysis workflow more efficient and concise.
 */

   val maxValueExpression=max(
     when(
       col("Release_Year") > 2000,
       col("Production_Budget"))
   )

 val conditionalAggDF =
   moviesDF
   .groupBy("Major_Genre")
   .agg(maxValueExpression.alias("Max_Production_Budget_Post_2000"))


  /**
   * Exercises
   *
   * 1. Sum up ALL the profits of ALL the movies in the DF
   * 2. Count how many distinct directors we have
   * 3. Show the mean and standard deviation of US gross revenue for the movies
   * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   * 5. For each Major Genre and Director, determine whether the director has made any movie
   *    that received a Rotten Tomatoes Rating ≥ 80. If yes, assign "Hit", else assign "Average".
   *    //hint: Conditional aggregation: Prioritize "Hit" if rating ≥ 80, else "Average"
   * 6. For each Major Genre, collect all distinct MPAA ratings used across its movies,
   *    and concatenate them as a comma-separated string (like a summary of allowed audience types per genre).
   * 7. Find highest-grossing movie in US per genre(groupBy genre) — but only for movies with IMDB rating ≥ 7.0
   */


  // 1
  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()

  // 2
  moviesDF
    .select(countDistinct(col("Director")))
    .show()

  // 3
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()

  // 4
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()

  import org.apache.spark.sql.functions._

  // Conditional aggregation: Prioritize "Hit" if rating ≥ 80, else "Average"
  val genreDirectorRatingPriorityDf = moviesDF
    .groupBy(col("Major_Genre"), col("Director"))
    .agg(
      max(when(col("Rotten_Tomatoes_Rating") >= 80, "Hit").otherwise("Average")).alias("Performance")
    )



  import org.apache.spark.sql.functions._

  val genreMpaaConcatDf = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      concat_ws(",", collect_set(col("MPAA_Rating"))).alias("All_MPAARatings")
    )

  import org.apache.spark.sql.functions._

  val highestGrossingHighRated = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      max(when(col("IMDB_Rating") >= 7.0, col("US_Gross")).alias("Top_US_Gross_HighRated"))
    )


}
