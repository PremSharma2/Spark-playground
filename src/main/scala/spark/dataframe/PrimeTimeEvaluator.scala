package spark.dataframe


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}

class PrimeTimeEvaluator {


  def addPrimeTime(movieDataframe: DataFrame): DataFrame = {

    movieDataframe.show()
    /**
     * Group by and aggregate the ticket sold per theatre,per year ,per week per movie
     */
    var df_agg_tickets_sold = movieDataframe.groupBy("theatre_name", "year", "week", "movie_id")
      .sum("ticket_sold")
    df_agg_tickets_sold = df_agg_tickets_sold.
      withColumnRenamed("sum(ticket_sold)", "ticket_sold")
    df_agg_tickets_sold.show()

    /**
     * Get the highest Grocer movie of the week
     */
    var df_highest_grocer = df_agg_tickets_sold.
      groupBy("theatre_name", "year", "week").max("ticket_sold")
    df_highest_grocer = df_highest_grocer.
      withColumnRenamed("max(ticket_sold)", "ticket_sold")
    df_highest_grocer.show()

    /**
     * Join to get the movie_id of the highest grocer movie of the week by year and theatre
     */
    val prime_movie = df_highest_grocer.
      join(df_agg_tickets_sold, Seq("theatre_name", "year", "week", "ticket_sold")).
      drop("ticket_sold")
    prime_movie.show()

    /**
     * renaming columns to avoid confusion
     */
    var renamedColumns = movieDataframe.columns.
      map(c => movieDataframe(c).as(s"orig_$c"))

    val df_orig_with_col_renamed = movieDataframe.select(renamedColumns: _*)

    /**
     * Join the highest grocer movie with original dataframe to add a new column is_prime_time.
     */
    var df_prime = df_orig_with_col_renamed.join(
      prime_movie,
      df_orig_with_col_renamed("orig_theatre_name") === prime_movie("theatre_name")
      && df_orig_with_col_renamed("orig_year") === prime_movie("year")
      && df_orig_with_col_renamed("orig_week") === prime_movie("week")
      && df_orig_with_col_renamed("orig_movie_id") === prime_movie("movie_id"), "left")
      .withColumn("is_prime_time", when(col("movie_id").isNull, "No").
        otherwise("Yes"))
       .drop(prime_movie("year"))
       .drop(prime_movie("week"))
       .drop(prime_movie("theatre_name"))
       .drop("movie_id")

    renamedColumns = df_prime.columns.map(c => col(c).as(c.replaceFirst("orig_", "")))
    df_prime = df_prime.select(renamedColumns: _*)

    df_prime
  }



}
