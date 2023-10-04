package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank

import org.apache.spark.sql.functions.dense_rank

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
object DataFrameTest extends App {

  def parseOrder(str: String): OrderItems = {
    val line = str.split(",")
    OrderItems(line(0).toInt, line(1).toInt, line(2).toInt, line(3).toInt, line(4).toFloat, line(5).toFloat)
  }

  def parseRDD(rdd: RDD[String]): RDD[OrderItems] = {
    val header = rdd.first
    // rdd.filter(_(0) != header(0)).map(parseOrder).cache()
    rdd.map(parseOrder).cache()
  }
  case class OrderItems(itemid: Int, orderItemOrderId: Int, productId: Int, quantity: Int, orderItemSubtotal: Float, productPrice: Float)

  System.setProperty("hadoop.home.dir", "C:/winutils");
  val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  val spark = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");
  val sparksession: SparkSession = SparkSession.builder.master("local").config(spark).getOrCreate()
  import sparksession.implicits._
  val sparkContext = sparksession.sparkContext
  val ordersrdd = sparkContext.textFile("D:/Spark_VM/data-set/data/retail_db/orders", 2)
  val orderItems = sparkContext.textFile("D:/Spark_VM/data-set/data/retail_db/order_items", 2)
  val rdd = sparkContext.textFile("D:/Spark_VM/data-set/data/retail_db/order_items")
  val data = parseRDD(rdd).toDF.cache()
  println(data.show)
  val gp=data.groupBy("orderItemOrderId","itemid","productId").max("productPrice")
  println(gp.show())
  //val df = sparksession.read.format("csv").option("header", "true").load("D:/Spark_VM/data-set/data/retail_db/order_items").as[OrderItems]
 val byorderItemOrderId_productPrice_Desc = Window.partitionBy('orderItemOrderId).orderBy('productPrice desc)
 val ranked = data.withColumn("rank", dense_rank.over(byorderItemOrderId_productPrice_Desc))
  println(data.show)
  ranked.filter(row => row(6)==3).show
  println(ranked.show())
  //calculate the count of orders on the basis of order status
  orderItems.take(2).foreach(println)
  //1,1,957,1,299.98,299.98

}