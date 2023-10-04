package spark.dataframe


import org.apache.spark.sql.SparkSession

object BucketingTest  extends App{

  import org.apache.spark.SparkConf

  System.setProperty("hadoop.home.dir", "C:/winutils")


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
  //spark.range(10e4.toLong).write.mode(SaveMode.Overwrite).saveAsTable("t10e4")
  //spark.range(10e6.toLong).write.mode(SaveMode.Overwrite).saveAsTable("t10e6")
  // Bucketing is enabled by default
    // Let's check it out anyway
    assert(spark.sessionState.conf.bucketingEnabled, "Bucketing disabled?!")

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  //val tables = spark.catalog.listTables.where(col("name") startsWith "t10")
 // tables show false

 // val t4 = spark.table("t10e4")
 // val t6 = spark.table("t10e6")
  //t4.join(t6, "id").foreach(_ => ())
  val large = spark.range(10e6.toLong)
  println(large.queryExecution.toRdd.getNumPartitions)
  import org.apache.spark.sql.SaveMode
  large.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode(SaveMode.Overwrite)
    .saveAsTable("bucketed_4_id")
  // That gives 8 (partitions/task writers) x 4 (buckets) = 32 files
  // With _SUCCESS extra file and the ls -l header "total 794624" that gives 34 files
  println(large.queryExecution.toRdd.getNumPartitions)

  Thread.sleep(Long.MaxValue)


}
