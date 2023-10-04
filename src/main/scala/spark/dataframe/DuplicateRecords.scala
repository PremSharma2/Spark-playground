package spark.dataframe


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DuplicateRecords  extends  App {
  System.setProperty("hadoop.home.dir", "C:/winutils");
  //val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  val spark = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");
  val sparksession: SparkSession = SparkSession.builder.master("local").config(spark).getOrCreate()

  def parseOrder(ab:(String, Int)): Account = {

    Account(ab._1.toString, ab._2.toInt)
  }


  case class Account(AcctId: String, DuplicateCount: Int)

  val acctSeq = List(("1", "Acc1"), ("1", "Acc1"), ("1", "Acc1"), ("2", "Acc2"), ("2", "Acc2"), ("3", "Acc3"))
  acctSeq
  // acctDF.show(false)
  val columnName = Seq("AcctId","Details")
  val acctDF= sparksession.createDataFrame(acctSeq).toDF(columnName:_*)
  val countsRDD: RDD[(String, Int)] =
    acctDF.rdd.
    map(rec => (rec(0), 1)).
    reduceByKey(_+_).
    map(rec=> (rec._1.toString, rec._2))
  def parseRDD(rdd: RDD[(String,Int)]): RDD[Account] = {

    countsRDD.map(parseOrder).cache()
  }
  val countRowRDD: RDD[Account] = parseRDD(countsRDD)
 val countsDF: DataFrame = sparksession.createDataFrame(countsRDD).toDF("AcctId","AcctCount")

  val accJoinedDF: DataFrame = acctDF.join(countsDF,
    acctDF.col("AcctId")===countsDF.
      col("AcctId"), "left_outer").
    select(acctDF("AcctId"), acctDF("Details"), countsDF("AcctCount"))
  acctDF show false
  countsDF show  false
  // countsDF1 show false
 accJoinedDF show false



}
