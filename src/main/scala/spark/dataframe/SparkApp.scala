package spark.dataframe


import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkApp {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = createSparkSession

    var df_orig = spark.read.format("csv").option("header", "true").load("C:/prem/prem/movie.csv")
    df_orig = df_orig
      .withColumn("ticket_sold", df_orig.col("ticket_sold").cast(IntegerType))
      .withColumn("movie_id", df_orig.col("movie_id").cast(IntegerType))
      .withColumn("year", df_orig.col("year").cast(IntegerType))
      .withColumn("week", df_orig.col("week").cast(IntegerType))

    val primeTimeEvaluator = new PrimeTimeEvaluator()
    val df: DataFrame =primeTimeEvaluator.addPrimeTime(df_orig)
    df.repartition(2).write.mode(SaveMode.Overwrite)
      .parquet("C:\\prem\\prem\\Data\\Warehouse-data")

    val df1 = spark.read.format("parquet")
      .load("C:\\prem\\prem\\Data\\Warehouse-data")

       df1.coalesce(1).write.mode(SaveMode.Overwrite).
        json("C:\\prem\\prem\\Data\\Warehouse-data\\coleasce")

    spark.stop()
  }

  private def createSparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Spark Movie ")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    spark
  }
}
