package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
object DailyRevenuePerProductId extends App{
  
  System.setProperty("hadoop.home.dir", "C:/winutils");
  val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  val spark = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");

  val sparksession: SparkSession = SparkSession.builder.master("local").config(spark).getOrCreate()
  val sparkContext = sparksession.sparkContext
  val ordersRDD = sparkContext.textFile("C:\\prem\\prem\\Data\\Spark_VM\\orders", 2)
   val orderItems = sparkContext.textFile("C:\\prem\\prem\\Data\\Spark_VM\\data-set\\data\\retail_db\\order_items", 2)
  val ordersRDDFiltered= ordersRDD.filter(order => order.split(",")(3) == "COMPLETE" || order.split(",")(3)=="CLOSED")

  ordersRDDFiltered.take(10).foreach(println)
  //(OrdreId,OrderDate)
  val ordresMap: RDD[(Int, String)] =ordersRDDFiltered.map{
    order: String =>
      {
        val o = order.split(",")
        (o(0).toInt,o(1))
      }
  }
  //(ordreItemOrderId,(productId,OrdreItemsubtotal))
  val orderItemsMap: RDD[(Int, (Int, Float))] = orderItems.map{
    orderitem: String =>
      {
        val oi = orderitem.split(",")
        (oi(1).toInt,(oi(2).toInt,oi(4).toFloat))
      }
  }
}