package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col
import spark.UDF
object DataValidation extends App {

  System.setProperty("hadoop.home.dir", "C:/winutils");
  val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  // val spark = new SparkConf().setAppName("Simple Application").setMaster(("local[*]"))
  val sparksession: SparkSession = SparkSession.builder.master("local").getOrCreate()
  val sparkContext = sparksession.sparkContext
  val seq = Seq(Row("!@ Hello World.", 1), Row("\" @Hi there.", 0))
  val seqrdd = sparkContext.parallelize(seq, 1)
  val schema = new StructType()
    .add(StructField("id", StringType, true))
    .add(StructField("val1", IntegerType, true))
  val df = sparksession.createDataFrame(seqrdd, schema)
  df.show()
  val regex = "[~!@#$^%&*\\(\\)_+={}\\[\\]|;:\"'<,>.?`/\\\\-]".r
  val removeudf = udf(UDF.remove_leading(regex.toString()))
  df.withColumn("id", removeudf.apply(col("id"))).show
  

}