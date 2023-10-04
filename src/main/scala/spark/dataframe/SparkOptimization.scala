package spark.dataframe


object SparkOptimization extends App {


  import org.apache.spark.SparkConf
  import org.apache.spark.rdd.RDD
  import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted}
  import org.apache.spark.sql.functions.{desc, month, year}
  import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

  System.setProperty("hadoop.home.dir", "C:/winutils");
  //val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  val spark = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");
  val sparksession: SparkSession =
    SparkSession.builder.master("local").config(spark).getOrCreate()
  val sc = sparksession.sparkContext
  var outputWritten = 0L
  var inputRecords = 0L
  val map = Map.empty[Int, Long]
  sc.addSparkListener(new SparkListener() {
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
      val stageId = stageCompleted.stageInfo.stageId
      val metrics = stageCompleted.stageInfo.taskMetrics
      if (metrics.inputMetrics != null) {
        inputRecords += metrics.inputMetrics.bytesRead
      }
      if (metrics.outputMetrics != null) {
        outputWritten += metrics.outputMetrics.bytesWritten
      }
      map.+(stageId -> outputWritten)
    }
  })

  import sparksession.sqlContext.implicits._

  case class Stock(dt: String, openprice: Double, highprice: Double, lowprice: Double, closeprice: Double, volume: Double, adjcloseprice: Double)

  def parseStock(str: String): Stock = {
    val line = str.split(",")
    Stock(line(0), line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble, line(5).toDouble, line(6).toDouble)
  }

  def parseRDD(rdd: RDD[String]): RDD[Stock] = {
    val header = rdd.first
    rdd.filter(_ (0) != header(0)).map(parseStock).cache()
  }

  val stocksAAONDF: DataFrame =
    parseRDD(sc.textFile("C:\\prem\\prem\\Data\\Spark_VM\\475_m6_StockMarket_DataSets\\StockMarket_DataSets\\AAON.csv")).toDF.cache()
  // Display Data Frame
  stocksAAONDF.show()

  // Average monthly closing
  val output: Dataset[Row] =
    stocksAAONDF.
      select(year($"dt").alias("yr"), month($"dt").alias("mo"), $"adjcloseprice").
      groupBy("yr", "mo").
      avg("adjcloseprice").
      orderBy(desc("yr"), desc("mo"))
  /*
  val studentsDF = Seq(
    ("mario", "italy", "europe"),
    ("stefano", "italy", "europe"),
    ("victor", "spain", "europe"),
    ("li", "china", "asia"),
    ("yuki", "japan", "asia"),
    ("vito", "italy", "europe")
  ).toDF("name", "country", "continent")

   */


  val sortedmap: List[(Int, Long)] = map.toList.sortBy(_._1)
  val outputTuple: (Int, Long) = sortedmap.last
  val outputRecords: Long = outputTuple._2
  val MEGABYTE: Long = 1024L * 1024L
  val outputDatasetSize: Long = outputRecords / MEGABYTE
  val numberOfPartitions: Long = (outputDatasetSize / 1024) + 1
  println(numberOfPartitions)
  output.repartition(numberOfPartitions.toInt)
  output.write.parquet("C:\\prem\\prem\\Data\\Spark_VM\\data-set\\spark-listenertest")
  println("outputWritten", outputWritten)


}
