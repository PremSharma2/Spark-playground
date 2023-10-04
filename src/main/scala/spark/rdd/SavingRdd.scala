package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
object SavingRdd extends App {
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
  val orderJsonsrdd = sparkContext.textFile("D:/Spark_VM/data-set/data/retail_db_json/orders", 2)
  val ordercountByStatus = ordersrdd.map(order => (order.split(",")(3), 1)).
    reduceByKey((total, element) => total + element)
  //ordercountByStatus.saveAsTextFile("D:/Spark_VM/data-set/save/rdd")

  //ordercountByStatus.saveAsTextFile("D:/Spark_VM/data-set/snappyCompression/rdd", classOf[org.apache.hadoop.io.compress.SnappyCodec])
    
      //val orderJsonDf = sparksession.read.json("D:/Spark_VM/data-set/data/retail_db_json/orders")
      //orderJsonDf.show
     // orderJsonDf.write.format("parquet").save("D:/Spark_VM/data-set/dataframe/save")
     // orderJsonDf.save("D:/Spark_VM/data-set/dataframe/save","parquet")

      val orderparquetDf = sparksession.read.parquet("D:/Spark_VM/data-set/dataframe/save")
      orderparquetDf.show()
      
    
}