package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
object RddMapOperation extends App {

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
  sparkContext.getConf.getAll.foreach(println)
  //sparkContext.hadoopConfiguration.
  val ordersrdd = sparkContext.textFile("D:/Spark_VM/data-set/data/retail_db/orders", 2)
  println(ordersrdd.first())
  val str = ordersrdd.first()
  println(str.split(",")(1).substring(0, 10))
  println(str.split(",")(1).substring(0, 10).replace("-", ""))
  println(str.split(",")(1).substring(0, 10).replace("-", "").toInt)
  val orderDates = ordersrdd.map((str:String) => {
    str.split(",")(1).substring(0, 10).replace("-", "").toInt
  })
  
  
  val orderDates1 = ordersrdd.map{
    (str :String)=> str.split(",")(1).substring(0, 10).replace("-", "").toInt
  }
  orderDates.take(10).foreach(println)
  
  
  sparkContext.stop()

}