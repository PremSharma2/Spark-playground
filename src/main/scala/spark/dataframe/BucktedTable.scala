package spark.dataframe


import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object BucktedTable extends  App {

  val sparkConf = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    .set("spark.sql.shuffle.partitions","2")
    .set("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse/warehouse2")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");
  val spark = SparkSession
    .builder()
    .appName("Simple Application ")
    .master("local")
    .config(sparkConf)
    .getOrCreate()
  spark.sparkContext.setLogLevel("Error")
  spark.sparkContext.setLogLevel("Error")

  spark.range(10e4.toLong)
    .write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode(SaveMode.Overwrite)
    .saveAsTable("bucketed_4_10e4")


  spark.range(10e6.toLong)
    .write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode(SaveMode.Overwrite)
    .saveAsTable("bucketed_4_10e6")

  val bucketed_4_10e4 = spark.table("bucketed_4_10e4")
  val bucketed_4_10e6 = spark.table("bucketed_4_10e6")
  val numPartitions = bucketed_4_10e4.queryExecution.toRdd.getNumPartitions
  assert(numPartitions == 4)

  /*

  There are however requirements that have to be met before Spark Optimizer gives a no-Exchange query plan:

The number of partitions on both sides of a join has to be exactly the same.

Both join operators have to use HashPartitioning partitioning scheme.

It is acceptable to use bucketing for one side of a join.
   */

}
