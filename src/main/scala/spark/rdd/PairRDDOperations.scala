package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
object PairRDDOperations extends App {
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
  val orderItems = sparkContext.textFile("D:/Spark_VM/data-set/data/retail_db/order_items", 2)
  //calculate the count of orders on the basis of order status
  //1,2013-07-25 00:00:00.0,11599,CLOSED
//2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
  ordersrdd.take(2).foreach(println)
  /*
   * 1,1,957,1,299.98,299.98l
     2,2,1073,1,199.99,199.99
   */
  orderItems.take(2).foreach(println)
  val orderMap: RDD[(String, String)] = ordersrdd.map {
    order => (order.split(",")(3), "") 
  }
  println(orderMap.countByKey())
  //find out txn of max revenue among all txn in orderItem dataset
  //we need to reduce all the values to single value i.e single max txn
  val orderItemsRevenue: RDD[Float] = orderItems.map {
    oi => oi.split(",")(4).toFloat
  }
  
  //def reduce(f: (Float, Float) => Float): Float
  /*
   * this reduce function works on each partition and finally on
   *  all partition values to give final reduced output
   */
  val orderItemsMaxRevenue = orderItemsRevenue.reduce {
    (accumlator, revenue) => if (accumlator < revenue) revenue else accumlator
  }
  println(orderItemsMaxRevenue)

  // Get revenue Per Order Id
 //1,1,957,1,299.98,299.98l
  val orderItemMap: RDD[(Int, Float)] = orderItems.map {
    orderitem => (orderitem.split(",")(1).toInt, orderitem.split(",")(4).toFloat)
  }

  /*
   * (1,299.98)
(2,129.99)
(2,199.99)
(2,250.0)
(4,49.98)
(4,150.0)
(4,199.92)
(4,299.95)
(5,99.96)
(5,129.99)
   *
   *
   */
  orderItemMap.takeOrdered(10).foreach(println)
  val order_sub_total_Item_group = orderItemMap.groupByKey()
  order_sub_total_Item_group.takeOrdered(10).foreach(println)
  val revenue_per_order = order_sub_total_Item_group.map {
    (tuple) => (tuple._1, tuple._2.toList.sum)
  }
  revenue_per_order.take(10).foreach(println)

  // Get data in descending order by order_item_sub_total_Item for each order_id
  //we need flattend result
  val sorteddata: RDD[(Int, Float)] = order_sub_total_Item_group.flatMap {

    (tuple) => tuple._2.toList.sortBy(o => -o).map(order_item_sub_total_Item => (tuple._1, order_item_sub_total_Item))
      
  }
  sorteddata.take(10).foreach(println)
  println(sorteddata.toDebugString)
  // total revenue per Order
  val revenuePerOrderId: RDD[(Int, Float)] = orderItemMap.reduceByKey {
    (total, revenue) => total + revenue
  }
  // min revenue  Orders
  val minrevenuePerOrderId = orderItemMap.reduceByKey {
    (min, revenue) => if (min > revenue) revenue else min
  }
  revenuePerOrderId.takeOrdered(10).foreach(println)
  minrevenuePerOrderId.takeOrdered(10).foreach(println)

  //Maximum Revenue on the basis of Product
  //input is (orderId,order_item_sub_total_Item)
  //desired o/p (orderId,(total_order_revenue,max_Of_order_item_sub_total_Item))
  // this lambda will generate the intermediate o/p across the partitions
  /*
   * (1,299.98)
(2,129.99)
(2,199.99)
(2,250.0)
(4,49.98)
(4,150.0)
(4,199.92)
(4,299.95)
(5,99.96)
(5,129.99)
   *
   *
   */

  //o/p will be
  /*
   *
   * (1,(299.98,299.98))
(2,(579.98,250.0))
(4,(699.85004,299.95))
(5,(1129.8601,299.98))
(7,(579.92004,299.98))
(8,(729.84,299.95))
(9,(599.96,199.99))
(10,(651.92,199.99))
(11,(919.79004,399.96))
(12,(1299.8701,499.95))
   */
  val seqOp = (accumlator: (Float, Float), sub_total_Item: Float) => (accumlator._1 + sub_total_Item, if (sub_total_Item > accumlator._2) sub_total_Item else accumlator._2)
  //combiner combining the o/p of intermediate accumlators operation and will generate final value
  val combOp = (accumlator1: (Float, Float), accumlator: (Float, Float)) => (accumlator1._1 + accumlator._1, if (accumlator1._2 > accumlator._2) accumlator._2 else accumlator1._2)
  val revenueandMaxPerProductId = orderItemMap.aggregateByKey((0.0f, 0.0f))(seqOp, combOp)
  revenueandMaxPerProductId.takeOrdered(10).foreach(println)
}