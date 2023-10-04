package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import java.util.Formatter.DateTime

object RddJoin extends App {
  
  case class Order(orderId: Int, orderDate:String, order_Customer_Id:Int,orderStatus:String)
// defining a case class "cust" with fields according to customer_data file. We have mentioned the data type as well.
  case class OrderItems(itemid: Int, orderItemOrderId: Int, productId: Int, quantity: Int, orderItemSubtotal: Float, productPrice: Float)

// defining a case class "OrderItems" with fields according to OrderItems file.

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
  orderItems.take(5).foreach(println)
  val orderspairRdd = ordersrdd.map {

    (order: String) =>
      {
        val o = order.split(",")
        (o(0).toInt, o(1).substring(0, 10).replace("-", "").toInt)

      }

  }

  orderspairRdd.take(5).foreach(println)

  val orderItemPairRdd = orderItems.map {

    (orderItem: String) =>
      {
        val o = orderItem.split(",")
        (o(1).toInt, orderItem)
      }

  }
  orderItemPairRdd.take(5).foreach(println)

  /*val l=List("hello","Let us prform word count ","to demonstrate the word count programme")
val rdd=sparkContext.parallelize(l, 1)
val l_map=rdd.map( element => element.split(""))
val l_flatmap=rdd.flatMap(element => element.split(""))
l_map.foreach(println)
l_flatmap.foreach(print)*/

  val ordersPair = ordersrdd.map {

    (order: String) =>
      {
        val o = order.split(",")
        (o(0).toInt, o(1).substring(0, 10).replace("-", ""))

      }

  }
  // get the statstistcs detail for an order along with the item associated with it and amount spent on that item
  // i.e orderid,(orederdate,(orderitemId,Itemtotal )
  ordersPair.take(5).foreach(println)

  val orderItemPair = orderItems.map {

    (orderItem: String) =>
      {
        val oi = orderItem.split(",")
        (oi(1).toInt, (oi(0), oi(4).toFloat))
      }

  }
  orderItemPair.take(5).foreach(println)

  val ordersJoin = ordersPair.join(orderItemPair)
//(41234,(20140404,(102921,109.94)))
  ordersJoin.take(10).foreach(println)
  println(ordersJoin.count)

  //Get all the orders which do not have corresponding entries in order items table
  // (oid,order)
  val orderspairrdd = ordersrdd.map {

    (order: String) =>
      {
        val o = order.split(",")
        (o(0).toInt, order)

      }

  }
  orderspairrdd.take(10).foreach(println)

  val orderItemPairrdd = orderItems.map {

    (orderItem: String) =>
      {
        val o = orderItem.split(",")
        (o(1).toInt, orderItem)
      }

  }
  orderItemPairrdd.take(10).foreach(println)

  val ordersLeftOuterJoin = orderspairrdd.leftOuterJoin(orderItemPairrdd)
  ordersLeftOuterJoin.take(100).foreach(println)
  //RDD[(Int, (String, Option[String]))]
  //(24688,(24688,2013-12-25 00:00:00.0,12022,COMPLETE,Some(61827,24688,957,1,299.98,299.98)))
  //(12420,(12420,2013-10-09 00:00:00.0,449,PENDING,None))
// selecting only none
  val ordersLeftOuterJoinFilter = ordersLeftOuterJoin.filter(order => order._2._2 == None)
  ordersLeftOuterJoinFilter.take(10).foreach(println)
    //(12420,(12420,2013-10-09 00:00:00.0,449,PENDING,None))
  val ordersWithNoOrderItem = ordersLeftOuterJoinFilter.map {
    order => (order._1, order._2._1)
  }
  ordersWithNoOrderItem.take(10).foreach(println)
 // (5354,5354,2013-08-26 00:00:00.0,7616,PENDING_PAYMENT)
}