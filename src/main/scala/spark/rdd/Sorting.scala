package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object Sorting extends App {

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
  val ordersrdd = sparkContext.textFile("D:/Spark_VM/data-set/data/retail_db/orders", 2)
  val products = sparkContext.textFile("D:/Spark_VM/data-set/data/retail_db/products", 2)
  products.take(10).foreach(println)
  //(productCategoryId,product)
  val productsMap = products.map {
    product => (product.split(",")(1).toInt, product)
  }

  productsMap.takeOrdered(10).foreach(println)

  val sorted = productsMap.sortByKey(false, 2)
  sorted.take(10).foreach(println)

  //sorting on Composite Key
  //(productCategoryId,productPrice)
  val productsMapforComposteKey = products.
      filter(product => product.split(",")(4) != "").
      map( product => ((product.split(",")(1).toInt,   product.split(",")(4).toFloat), product))
     
    
  val sorted1 = productsMapforComposteKey.sortByKey(true, 2)
  sorted1.take(10).foreach(println)
  
  val productsMapforComposteKey1 = products.
      filter(product => product.split(",")(4) != "").
      map( product => ((product.split(",")(1).toInt, -product.split(",")(4).toFloat), product))
     
    
  val sorted2 = productsMapforComposteKey1.sortByKey(true, 2).map(rec=> rec._2)
  sorted2.take(10).foreach(println)
  //Ranking - Global (details of top 5 prodicts )
  
   val productsMapPriceAsKey = products.
      filter(product => product.split(",")(4) != "").
      map( product => ((product.split(",")(4).toFloat, product)))
      
      val sorted3 = productsMapPriceAsKey.sortByKey(false, 2)
  sorted3.take(10).foreach(println)
  // use of takeOrdred
  val top10Products=products.
      filter(product => product.split(",")(4) != "").
      takeOrdered(10)(Ordering[Float].reverse.on(product=>product.split(",")(4).toFloat))
      top10Products.foreach(println)
} 