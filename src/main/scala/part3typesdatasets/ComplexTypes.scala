package part3typesdatasets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}


object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //TODO Paying with Dates In spark
val dateExpression: Column =  to_date(col("Release_Date"), "dd-MMM-yy")
  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), dateExpression.as("Actual_Release")) // conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub

val filterExpression: Column = col("Actual_Release").isNull
  moviesWithReleaseDates.select("*").where(filterExpression)

  /**
    * Exercise
    * 1. How do we deal with multiple date formats?
    * 2. Read the stocks DF and parse the dates
    */

  // 1 - parse the DF multiple times, then union the small DFs

  // 2
  val stocksDF = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")

  val stocksDFWithDates = stocksDF
    .withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))

  // TODO Structures

  // TODO 1 - with col operators combining two columns into single column
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  //TODO 2 - with expression strings
  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  //TODO:-> Arrays

  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // ARRAY of strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"), // indexing
    size(col("Title_Words")), // array size
    array_contains(col("Title_Words"), "Love") // look for value in array
  )

  val emp = Seq((1, "AAA", "dept1", 1000, "2019-02-01 15:12:13"),
  (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
  (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
  (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
  (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
  (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
  (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
  (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
  (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
  (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17"))
 val  empdf = spark.createDataFrame(emp).toDF("id","Name","Department","Salary","Date")
  /*
TODO
     add_months
    This function adds months to a date.
    It will return a new date,
    however many months from the start date.
    In the below statement we add 1 month to the column “date”
    and generated a new column as “next_month”.
   */
  val df = empdf
    .select("Date")
    .withColumn("next_month", add_months(col("Date"), 1))
  df.show(2)

  val df1 = empdf
    .withColumn("current_timestamp", current_timestamp())
    .select("id", "current_timestamp")
  df1.show(false)

  /*
TODO
    date_add
    This function returns a date x days after the start date passed to the function.
     In the example below, it returns a date 5 days after “date”
     in a new column as “next_date”. E.g. for date: 1st Feb 2019 it returns 6th Feb 2019.
   */

  val df3 = empdf
    .select("Date")
    .withColumn("next_date", date_add(col("Date"), 5))
  df3.show(2)

  /*
TODO
   date_format
 This function will convert the date to the specified format.
 For example, we can convert the date from “yyyy-MM-dd” to “dd/MM/yyyy” format.
   */

  val df4 = empdf
    .select("Date")
    .withColumn("new_date", date_format(col("Date"), "dd/MM/yyyy"))
  df4.show(2)

  /*
TODO
    This function returns a date some number of the days before the date passed to it
 . It is the opposite of date_add. In the example below,
  it returns a date that is 5 days earlier in a column as “new_date”.
  For example, date 1st Feb 2019 returns 27th Jan 2019.
   */
  val df5 = (empdf
    .select("Date")
    .withColumn("new_date", date_sub(col("Date"), 5)))
  df5.show(2)

  /*
TODO
   This function returns a timestamp truncated to the specified unit.
   It could be a year, month, day, hour, minute, second, week or quarter.
 Let’s truncate the date by a year.
 we can use “yyyy” or “yy” or” “year” to specify year.
 For timestamp “2019–02–01 15:12:13”,
  if we truncate based on the year it will return “2019–01–01 00:00:00”
   */

  val df6 = empdf
    .select("Date")
    .withColumn("new_date", date_trunc("year", col("Date")))

  df6.show(2)

  /*
TODO
   Let’s truncate date by month.
   We use “mm” or “month” or” “mon” to specify month.
   For timestamp “2019–02–01 15:12:13”,
   if we truncate based on month it returns “2019–02–01 00:00:00”
   */
 val df7 = empdf
    .select("Date")
    .withColumn("new_date", date_trunc("month", col("Date")))
  df7.show(2)

  /*
todo
  Let’s truncate date by day.
  We can use “day” or “dd” to specify day.
  For timestamp “2019–02–10 15:12:13”,
  if we truncate based on day it will return “2019–02–10 00:00:00”
   */
  val df8 = empdf
    .select("Date")
    .withColumn("new_date", date_trunc("day", col("Date")))
  df8.show(2)
  /*
 TODO
  This function returns the day of the month. For 5th Jan 2019 (2019–01–05) it will return 5.
   */
 val df9 = empdf
    .select("Date")
    .withColumn("dayofmonth", dayofmonth(col("Date")))
  df9.show(2)

  /*
TODO
 This function returns the day of the week as an integer.
 It will consider Sunday as 1st and Saturday as 7th.
 For 1st Feb 2019 (2019–02–01) which is Friday,
 it will return 6. Similarly,
 for 1st April 2018 (2018–04–01) which is Sunday, it will return 1.
   */

  val df10 = empdf
    .select("Date")
    .withColumn("dayofweek", dayofweek(col("Date")))
  df10.show(2)

  /*
TODO
 This function returns the day of the year as an integer.
 For 1st Feb it will return 32 (31 days of Jan +1 day of Feb).
 For 1st April 2018,
 it will return 91 (31 days of Jan + 28 days of Feb (2018 is a non-leap year) + 31 days of Mar + 1 day of April).
   */
  val df11 = empdf
    .select("Date")
    .withColumn("dayofyear", dayofyear(col("Date")))
  df11.show(2)

  /*
TODO
 This function converts UTC timestamps to timestamps of any specified timezone.
 By default, it assumes the date is a UTC timestamp.
 Let's convert a UTC timestamp to “PST” time.
   */

 val df12 = empdf
    .select("Date")
    .withColumn("pst_timestamp", from_utc_timestamp(col("Date"), "PST"))
  df12.show(2)

  /*
TODO
  This function converts timestamp strings of the given format to Unix timestamps (in seconds).
  The default format is “yyyy-MM-dd HH:mm:ss”.
 (Note: You can use spark property: “spark.sql.session.timeZone” to set the timezone.)
 -------------------+--------------+
|               date|unix_timestamp|
+-------------------+--------------+
|2019-02-01 15:12:13|    1549033933|
|  2018-04-01 5:12:3|    1522559523|
+-------------------+--------------+
   */
val  df13= empdf
    .select("Date")
    .withColumn("unix_timestamp", unix_timestamp(col("Date"), "yyyy-MM-dd HH:mm:ss"))
  df13.show(2)

  /*
TODO
 This function converts the number of seconds from Unix epoch (1970–01–01 00:00:00 UTC)
 to a given string format.
  You can set the timezone and format as well.
  (Note: you can use spark property: “spark.sql.session.timeZone” to set the timezone).
  For demonstration purposes, we have converted the timestamp to Unix timestamp and converted it back to timestamp.
   */

  val df14 = empdf
    .select("Date")
  // Convert timestamp to unix timestamp.
    .withColumn("unix_timestamp", unix_timestamp(col("Date"), "yyyy-MM-dd HH:mm:ss"))
  // Convert unix timestamp to timestamp.
    .withColumn("date_from_unixtime", from_unixtime(col("unix_timestamp")))
  df14.show(2)

  //TODO This function will return the hour part of the date.

  val df15 = empdf
    .select("Date")
    .withColumn("hour", hour(col("Date")))
  df15.show(2)
  /*
TODO
  This function will return the last date of the month for a given date.
  For 5th Jan 2019, it will return 31st Jan 2019,
  since this is the last date for the month.
   */
  val df16 = empdf.select("Date").withColumn("last_date", last_day(col("date")))

//TODO This function will return minute part of the date.
 val df17 = empdf
    .select("Date")
    .withColumn("minute", minute(col("Date")))
  df17.show(2)

//TODO This function will return the month part of the date.
  /*
  +-------------------+-----+
|               date|month|
+-------------------+-----+
|2019-02-01 15:12:13|    2|
|  2018-04-01 5:12:3|    4|
+-------------------+-----+
   */
 val df18 = empdf
    .select("Date")
    .withColumn("month", month(col("Date")))
  df18.show(2)


/*
TODO
 This function returns the difference between dates in terms of months.
 If the first date is greater than the second one,
 the result will be positive else negative.
  For example, between 6th Feb 2019 and 5th Jan 2019, it will return 1.
 */

 val df19 = empdf
    .select("Date")
  // Add another date column as current date.
    .withColumn("current_date", current_date())
  // Take the difference between current_date and date column in terms of months.
  .withColumn("months_between", months_between(col("current_date"), col("Date")))
  df19.show(2)

/*
TODO
  This function will return the next day based on the dayOfWeek specified in the next argument.
  For e.g. for 1st Feb 2019 (Friday) if we ask for next_day as Sunday, it will return 3rd Feb 2019.
  +-------------------+----------+
|               date|  next_day|
+-------------------+----------+
|2019-02-01 15:12:13|2019-02-03|
|  2018-04-01 5:12:3|2018-04-08|
+-------------------+----------+
 */
  val df20 = empdf
    .select("Date")
    .withColumn("next_day", next_day(col("Date"), "sun"))
  df20.show(2)

//TODO This function will return a quarter of the given date as an integer.
  /*
  +-------------------+-------+
|               date|quarter|
+-------------------+-------+
|2019-02-01 15:12:13|      1|
|  2018-04-01 5:12:3|      2|
+-------------------+-------+
   */
 val df21 = empdf
    .select("Date")
    .withColumn("quarter", quarter(col("Date")))
  df21.show(2)


/*
TODO
  This function will return the second part of the date.
  +-------------------+------+
 |               date|second|
 +-------------------+------+
 |2019-02-01 15:12:13|    13|
 |  2018-04-01 5:12:3|     3|
 +-------------------+------+
 */

 val df22 = empdf
    .select("Date")
    .withColumn("second", second(col("Date")))
  df22.show(2)

/*
TODO
  This function will convert the String or TimeStamp to Date.
  Note: Check the data type of column “date” and “to-date”.
 If the string format is ‘yyyy-MM-dd HH:mm:ss’
 then we need not specify the format. Otherwise,
 specify the format as the second arg in to_date function.
 +-------------------+----------+
|               date|   to_date|
+-------------------+----------+
|2019-02-01 15:12:13|2019-02-01|
|  2018-04-01 5:12:3|2018-04-01|
+-------------------+----------+
 */

 val df23 = empdf
    .select("Date")
    .withColumn("to_date", to_date(col("Date")))
  df23.show(2)



/*
TODO
 This function converts String to TimeStamp.
 Here we convert a string of format ‘dd/MM/yyyy HH:mm:ss’
 to the “timestamp” data type. The default format is ‘yyyy-MM-dd HH:mm:ss’
 */
  val df25 = df22
    .withColumn("new_date", to_date(col("Date"), "dd/MM/yyyy HH:mm:ss"))
  df25.show(2)

/*
TODO
  This function converts given timestamp to UTC timestamp.
  Let's convert a “PST” timestamp to “UTC” timestamp.
  +-------------------+-------------------+
|               date|      utc_timestamp|
+-------------------+-------------------+
|2019-02-01 15:12:13|2019-02-01 23:12:13|
|  2018-04-01 5:12:3|2018-04-01 12:12:03|
+-------------------+-------------------+
 */

  val df26 = empdf
    .select("date")
    .withColumn("utc_timestamp", to_utc_timestamp(col("Date"), "PST"))
  df26.show(2)

/*
TODO
   This function will return the weekofyear for the given date.
   +-------------------+----------+
|               date|weekofyear|
+-------------------+----------+
|2019-02-01 15:12:13|         5|
|  2018-04-01 5:12:3|        13|
+-------------------+----------+
 */

  val df27 = empdf
    .select("date")
    .withColumn("weekofyear", weekofyear(col("Date")))
  df27.show(2)


/*
TODO
  This function will return the year part of the date.
   +-------------------+----+
|               date|year|
+-------------------+----+
|2019-02-01 15:12:13|2019|
|  2018-04-01 5:12:3|2018|
+-------------------+----+
 */
 val df28 = empdf
    .select("date")
    .withColumn("year", year(col("Date")))
  df28.show(2)
}
