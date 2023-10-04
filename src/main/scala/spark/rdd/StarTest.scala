package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.{broadcast, max, row_number}
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
object StarTest extends App {

  def parseOrder(str: String): MovieData = {
    val line = str.split(",")
    MovieData(line(0).toInt, line(1).toInt, line(2).toInt, line(3), line(4).toInt, line(5), line(6).toInt)
  }

  def parseRDD(rdd: RDD[String]): RDD[MovieData] = {
    val header = rdd.first
    // rdd.filter(_(0) != header(0)).map(parseOrder).cache()
    rdd.map(parseOrder).cache()
  }
  case class MovieData(movieId: Int, movieStartTime: Int, movieEndTime: Int, movie_run_date: String, week: Int, year: String, numOfTicketsSold: Int)

  System.setProperty("hadoop.home.dir", "C:/winutils");
  val logFile = "D:/Spark_VM/moviedata.txt" // Should be some file on your system
  val spark = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    .set("spark.sql.shuffle.partitions","2")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");
  val sparksession: SparkSession = SparkSession.builder.master("local").config(spark).getOrCreate()
  import sparksession.implicits._
  val sparkContext = sparksession.sparkContext
  val movierdd = sparkContext.textFile("D:/Spark_VM/movie.csv", 2)

  val DF: DataFrame = parseRDD(movierdd).toDF.cache()
  
  DF.show()

  /*
   * +-------+--------------+------------+--------------+----+----+----------------+
|movieId|movieStartTime|movieEndTime|movie_run_date|week|year|numOfTicketsSold|
+-------+--------------+------------+--------------+----+----+----------------+
|   7499|             9|          12|     12/3/2019|   3|2018|           32000|
|   7499|            12|           6|     12/3/2019|   3|2018|           10000|
|   7499|             6|           9|     12/3/2019|   3|2018|           11000|
|   7499|             9|          12|    13-03-2019|   3|2018|            8000|
|   7499|            12|           6|    13-03-2019|   3|2018|             300|
|   7499|             6|           9|    13-03-2019|   3|2018|            3009|
|   7499|             9|          12|    14-03-2019|   3|2018|            3000|
|   7499|            12|           6|    14-03-2019|   3|2018|            4000|
|   7499|             6|           9|   14-03-2019 |   3|2018|            2000|
|   7499|             9|          12|    19-03-2019|   4|2018|            6000|
|   7499|            12|           6|    19-03-2019|   4|2018|            5000|
|   7499|             6|           9|   19-03-2019 |   4|2018|           32000|
|   7499|             9|          12|    20-03-2019|   4|2018|           10000|
|   7499|            12|           6|    20-03-2019|   4|2018|           11000|
|   7499|             6|           9|   20-03-2019 |   4|2018|            8000|
|   7499|             9|          12|    21-03-2019|   4|2018|           30000|
|   7499|            12|           6|    21-03-2019|   4|2018|            4000|
|   7499|             6|           9|    21-03-2019|   4|2018|            2000|
+-------+--------------+------------+--------------+----+----+----------------+
   */
 
  val modifiedDF = DF.groupBy($"week".as("weekWindow"), $"movieStartTime".as("startTime"),
                   $"movieEndTime".as("endTime"),$"movieId".as("mveId")).
                   agg(max("numOfTicketsSold").as("maxmTicketSold")).orderBy($"weekWindow")
  modifiedDF.show()
  /*
   * +----------+---------+-------+---------------------+
+----------+---------+-------+-----+--------------+
|weekWindow|startTime|endTime|mveId|maxmTicketSold|
+----------+---------+-------+-----+--------------+
|         3|       12|      6| 7499|         10000|
|         3|        9|     12| 7499|         32000|
|         3|        6|      9| 7499|         11000|
|         4|        9|     12| 7499|         30000|
|         4|       12|      6| 7499|         11000|
|         4|        6|      9| 7499|         32000|
+----------+---------+-------+-----+--------------+

   * 
   */
  
  val newModified= modifiedDF.groupBy($"weekWindow".as("weekWindow1")).agg(max("maxmTicketSold").
  
  as("max_ticket_perweek"))
  newModified.show()
 /*
 +-----------+------------------+
|weekWindow1|max_ticket_perweek|
+-----------+------------------+
|          3|             32000|
|          4|             32000|
+-----------+------------------+
  */
  
  /*val dfTopByJoin = DF.join(  
    broadcast(newModified),
    ($"movieStartTime" === $"movieStartTime") && ($"movieEndTime" === $"movieEndTime") && ($"week" === $"week"))
    .drop("start_time")
    .drop("endtime")
    */
  val dfTopByJoin = DF.join(  
    newModified,
    (DF.col("week") === newModified.col("weekWindow1")) && 
    (DF.col("numOfTicketsSold") === newModified.col("max_ticket_perweek")) ).
    //select(newModified.columns.map(newModified.apply(_).+("1")) : _*).
     select(DF.columns.map(DF.apply(_)) : _*)
     
    
    //.drop("week")  
    //.drop("max(maxmTicketSold)")
    //.drop("numOfTicketsSold")
    //.drop("movieStartTime")
    //.drop("movieEndTime")
    //.drop("movieId")
    //.withColumn("isPrimeslot", ($"week" === $"weekWindow1") && ($"numOfTicketsSold" === $"max(maxmTicketSold)")
dfTopByJoin.show


/*
+-------+--------------+------------+--------------+----+----+----------------+
|movieId|movieStartTime|movieEndTime|movie_run_date|week|year|numOfTicketsSold|
+-------+--------------+------------+--------------+----+----+----------------+
|   7499|             9|          12|     12/3/2019|   3|2018|           32000|
|   7499|             6|           9|   19-03-2019 |   4|2018|           32000|
+-------+--------------+------------+--------------+----+----+----------------+
 * 
 */


val result2 = DF.join(dfTopByJoin,(DF.col("week") === dfTopByJoin.col("week")) && 
                (DF.col("movieStartTime") === dfTopByJoin.col("movieStartTime")  &&
                 DF.col("movieId") === dfTopByJoin.col("movieId")&&
                 DF.col("numOfTicketsSold") === dfTopByJoin.col("numOfTicketsSold")&&
                 DF.col("movieEndTime") === dfTopByJoin.col("movieEndTime")),"left_Outer" ).
                 select(DF.columns.map(DF.apply(_)) : _*)
                 .withColumn("isPrimeSlot", 
                  when((DF.col("movieId") === dfTopByJoin.col("movieId")) &&
                       (DF.col("week") === dfTopByJoin.col("week")) &&
                       (DF.col("numOfTicketsSold") === dfTopByJoin.col("numOfTicketsSold")) &&
                       (DF.col("movieStartTime")===dfTopByJoin.col("movieStartTime")) && 
                       (DF.col("movieEndTime")=== dfTopByJoin.col("movieEndTime")).isNotNull ,"YES").
                       otherwise("NO"))
                       
                  /*otherwise(when( (DF.col("week") =!= dfTopByJoin.col("week")) &&
                       (DF.col("movieId") === dfTopByJoin.col("movieId"))&&
                       (DF.col("numOfTicketsSold") =!= dfTopByJoin.col("numOfTicketsSold")) &&
                       (DF.col("movieStartTime") =!= dfTopByJoin.col("movieStartTime")) && 
                       (DF.col("movieEndTime") =!= dfTopByJoin.col("movieEndTime")) ,"no")))*/
                  

  println(result2.show)
  
  
  
  /*
   * +-------+--------------+------------+--------------+----+----+----------------+-----------+
|movieId|movieStartTime|movieEndTime|movie_run_date|week|year|numOfTicketsSold|isPrimeSlot|
+-------+--------------+------------+--------------+----+----+----------------+-----------+
|   7499|             9|          12|     12/3/2019|   3|2018|           32000|        YES|
|   7499|            12|           6|     12/3/2019|   3|2018|           10000|        YES|
|   7499|             6|           9|     12/3/2019|   3|2018|           11000|        YES|
|   7499|             9|          12|    13-03-2019|   3|2018|            8000|        YES|
|   7499|            12|           6|    13-03-2019|   3|2018|             300|        YES|
|   7499|             6|           9|    13-03-2019|   3|2018|            3009|        YES|
|   7499|             9|          12|    14-03-2019|   3|2018|            3000|        YES|
|   7499|            12|           6|    14-03-2019|   3|2018|            4000|        YES|
|   7499|             6|           9|   14-03-2019 |   3|2018|            2000|        YES|
|   7499|             9|          12|    19-03-2019|   4|2018|            6000|        YES|
|   7499|            12|           6|    19-03-2019|   4|2018|            5000|        YES|
|   7499|             6|           9|   19-03-2019 |   4|2018|           32000|        YES|
|   7499|             9|          12|    20-03-2019|   4|2018|           10000|        YES|
|   7499|            12|           6|    20-03-2019|   4|2018|           11000|        YES|
|   7499|             6|           9|   20-03-2019 |   4|2018|            8000|        YES|
|   7499|             9|          12|    21-03-2019|   4|2018|           30000|        YES|
|   7499|            12|           6|    21-03-2019|   4|2018|            4000|        YES|
|   7499|             6|           9|    21-03-2019|   4|2018|            2000|        YES|
+-------+--------------+------------+--------------+----+----+----------------+-----------+
   */

}