package spark.rdd


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object SetOperations extends App {

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

  //Get All customers who placed ordres in 2013 August and 2013 Sept
  //Get All unique customers who placed orders in 2013 aug and sep 2013

  ////Customers who placed Order in aug 2013
  val custome_2013Aug = ordersrdd.filter(order => order.split(",")(1).contains("2013-08")).
    map(order => order.split(",")(2).toInt)
  println(custome_2013Aug.distinct().count())
  custome_2013Aug.takeOrdered(5).foreach(println)
  //Customers who placed Order in Sept 2013

  val custome_2013Sept = ordersrdd.filter(order => order.split(",")(1).contains("2013-09")).
    map(order => order.split(",")(2).toInt)
  println(custome_2013Sept.distinct().count())
  custome_2013Sept.takeOrdered(5).foreach(println)

  //Get All customers who placed ordres in 2013 August and 2013 Sept Both

  val customer_2013Aug_Sept2013 = custome_2013Aug.intersection(custome_2013Sept)
  println(customer_2013Aug_Sept2013.distinct().count())
  customer_2013Aug_Sept2013.takeOrdered(5).foreach(println)
  
  //Get All customers who placed ordres in 2013 August or in 2013 Sept 
  
  
  val customer_2013Aug_Or_Sept2013_union = custome_2013Aug.union(custome_2013Sept).distinct()
  println(customer_2013Aug_Or_Sept2013_union.distinct().count())
  customer_2013Aug_Or_Sept2013_union.takeOrdered(5).foreach(println)
  
  //Get All customers who placed ordres in 2013 August but not  in 2013 Sept 
  /*
  leftOuterJoin(custome_2013Sept.map(rec => (rec,1))).
                                                       filter(rec => rec._2._2.isEmpty)
   */
  //custome_2013Aug :RDD[Int]
  //custome_2013Sept :RDD[Int]
  val customer_2013Aug_minus_Sept2013 = custome_2013Aug
    .map(c => (c,1))
    .leftOuterJoin(custome_2013Sept.map(rec => (rec, 1)))
    .filter(_._2._2.isEmpty)
    .map(_._1)




  println(customer_2013Aug_minus_Sept2013.distinct().count())
  customer_2013Aug_minus_Sept2013.takeOrdered(5).foreach(println)
  
}