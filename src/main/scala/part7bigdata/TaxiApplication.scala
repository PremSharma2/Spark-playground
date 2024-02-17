package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object TaxiApplication extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()
  import spark.implicits._

  //val bigTaxiDF = spark.read.load("path/to/your/dataset/NYC_taxi_2009-2016.parquet")

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  taxiZonesDF.printSchema()

  /**
    * Questions:
    *
    * 1. Which zones have the most pickups/dropoffs overall?
    * 2. What are the peak hours for taxi?
    * 3. How are the trips distributed by length? Why are people taking the cab?
    * 4. What are the peak hours for long/short trips?
    * 5. What are the top 3 pickup/dropoff zones for long/short trips?
    * 6. How are people paying for the ride, on long/short trips?
    * 7. How is the payment type evolving with time?
    * 8. Can we explore a ride-sharing opportunity by grouping close short trips?
    *
    */

  // 1
  val pickupsByTaxiZoneDF = taxiDF.groupBy("PULocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("totalTrips").desc_nulls_last)

  // 1b - group by borough
  val pickupsByBoroughDF = pickupsByTaxiZoneDF.groupBy(col("Borough"))
    .agg(sum(col("totalTrips")).as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)


  // 2
  val pickupsByHourDF = taxiDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  //pickupsByHourDF.show(false)


  // 3
  val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30
  val tripDistanceStatsDF = tripDistanceDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("distance").as("mean"),
    stddev("distance").as("stddev"),
    min("distance").as("min"),
    max("distance").as("max")
  )

  //tripDistanceStatsDF.show

  val booleanExpression: Column =col("trip_distance") >= longDistanceThreshold
  val tripsWithLengthDF = taxiDF.withColumn("isLong", booleanExpression)

  val tripsWithLengthDF1= taxiDF.select(
    col("*"),
    booleanExpression.as("isLong")
  )

  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()

  //tripsByLengthDF.show

  // 4
  val pickupsByHourByLengthDF = tripsWithLengthDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day", "isLong")
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  //pickupsByHourByLengthDF.show

  // 5
  def pickupDropoffPopularity(predicateExpression: Column) =
    tripsWithLengthDF
    .where(predicateExpression)
    .groupBy("PULocationID", "DOLocationID").agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Pickup_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Dropoff_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .drop("PULocationID", "DOLocationID")
    .orderBy(col("totalTrips").desc_nulls_last)

  //pickupDropoffPopularity(not(col("isLong"))).show
  //pickupDropoffPopularity(col("isLong"))).show


  // 6
  val ratecodeDistributionDF = taxiDF
    .groupBy(col("RatecodeID")).agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)
  //ratecodeDistributionDF.show

  // 7
  val ratecodeEvolution = taxiDF
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("pickup_day"))
 // ratecodeEvolution.show

  // 8
  val passengerCountDF = taxiDF.where(col("passenger_count") < 3).select(count("*"))

  taxiDF.select(count("*"))
  val timestampExpression=round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer")
  val groupAttemptsDF =
    taxiDF
    .select(timestampExpression.as("fiveMinWindow"), col("PULocationID"), col("total_amount"))
    .where(col("passenger_count") < 3)
    .groupBy(col("fiveMinWindow"), col("PULocationID"))
    .agg(count("*").as("total_trips"), sum(col("total_amount")).as("total_amount"))
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinWindow") * 300))
    .drop("fiveMinWindow")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")

  //groupAttemptsDF.show(false)

  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))


  groupingEstimateEconomicImpactDF.show
  val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  // 40k/day = 12 million/year!!!
  totalProfitDF.show

}
