package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object Ranking extends App {

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

  /*
   * Get top N priced products 	with in each product category
   *
   */

  val productsMap = products.filter(product => product.split(",")(4) != "").
    map(product => (product.split(",")(1).toInt, product))

  println(productsMap.take(10).foreach(println))
  //(2,1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy)
  val productsGroupByCategory = productsMap.groupByKey()
  println(productsGroupByCategory.first())
  val productsIterableFromCategory = productsGroupByCategory.first()._2
  println(productsIterableFromCategory)
  val allProductsPricesAssociatedToSingleCategory = productsIterableFromCategory.map(p => p.split(",")(4).toFloat).toSet
  val sortedProductsPricesAssociatedToSingleCategory = allProductsPricesAssociatedToSingleCategory.toList.sortBy(price => -price)
  val topNPrices = sortedProductsPricesAssociatedToSingleCategory.take(5)
  print(topNPrices)
  val top5PricedProductsPerCategory = productsGroupByCategory.flatMap(rec => getTopNPricedProductsForEachCategory(rec._2.toList, 5))
  val productinEachCategorywithTopNPrices = productsGroupByCategory.mapValues(product => getTopNPricedProductsForEachCategory(product.toList, 5))
  productinEachCategorywithTopNPrices.takeOrdered(3).foreach(println)
  top5PricedProductsPerCategory.takeOrdered(3).foreach(println)
  // List of topN Prices is List(169.99, 149.99, 139.99, 129.99, 99.99)
  //o/p is (2,List(16,2,Riddell Youth 360 Custom Football Helmet,,299.99,http://images.acmesports.sports/Riddell+Youth+360+Custom+Football+Helmet, 11,2,Fitness Gear 300 lb Olympic Weight Set,,209.99,http://images.acmesports.sports/Fitness+Gear+300+lb+Olympic+Weight+Set, 5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet, 14,2,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,,199.99,http://images.acmesports.sports/Quik+Shade+Summit+SX170+10+FT.+x+10+FT.+Canopy, 12,2,Under Armour Men's Highlight MC Alter Ego Fla,,139.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Alter+Ego+Flash+Football..., 23,2,Under Armour Men's Highlight MC Alter Ego Hul,,139.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Alter+Ego+Hulk+Football..., 6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat))

  //Get all the Products in descending order by price
  //val productsSortedIterable =productsIterableFromCategory.toList.sortBy(product => -product.split(",")(4).toFloat)
  // val minOfTopNprices= topNPrices.min
  //val allProductsPricesAssociatedToSingleCategory=productsIterableFromCategory.map(p => p.split(",")(4).toFloat).toSet
  // allProductsPricesAssociatedToSingleCategory.foreach(println)
  // val sortedProductsPricesAssociatedToSingleCategory= allProductsPricesAssociatedToSingleCategory.toList.sortBy(price=> -price)
  def getTopNPricedProductsForEachCategory(productsIterableFromCategory: Iterable[String], topN: Int): Iterable[String] = {
    val allProductsPricesAssociatedToSingleCategory = productsIterableFromCategory.map(p => p.split(",")(4).toFloat).toSet
    val productsSortedIterable = productsIterableFromCategory.toList.sortBy(product => -product.split(",")(4).toFloat)
    val sortedProductsPricesAssociatedToSingleCategory = allProductsPricesAssociatedToSingleCategory.toList.sortBy(price => -price)
    val topNPrices = sortedProductsPricesAssociatedToSingleCategory.take(5)
    val minOfTopNPrices = topNPrices.min
    val topNPricedProducts = productsSortedIterable.takeWhile(product => product.split(",")(4).toFloat >= minOfTopNPrices)
    topNPricedProducts

  }
}