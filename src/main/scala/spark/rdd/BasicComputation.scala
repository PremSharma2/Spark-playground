package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
object BasicComputation extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils");
  val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  val sparkConfiguration = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");

  val sparksession: SparkSession = SparkSession.builder.master("local").config(sparkConfiguration).getOrCreate()
  val sparkContext = sparksession.sparkContext
  val ordersrdd = sparkContext.textFile("C:\\prem\\prem\\Data\\Spark_VM\\data-set\\data\\retail_db\\orders", 2)

  ordersrdd.take(5).foreach(println)
  val filteredorder = ordersrdd.filter(order => order.split(",")(3) == "COMPLETE")
  //filteredorder.foreach(println)
  println(filteredorder.count)
  val s = ordersrdd.first
  println(s.contains("COMPLETE") || s.contains("CLOSED"))

  val orderstatusRDD = ordersrdd.map(order => order.split(",").apply(3)).distinct()
  orderstatusRDD.foreach(println)

  val filteredRDD = ordersrdd.filter {
    //lamda function
    order: String =>
      {
        val o = order.split(",")
        ((o.apply(3) == "COMPLETE") || (o.apply(3) == "CLOSED") && (o.apply(1).contains(" 2013-09 ")))
      }
  }
  filteredRDD.take(10).foreach(println)
}