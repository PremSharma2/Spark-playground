package spark.dataframe


import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, regexp_extract}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.matching.Regex
object SplittingDataframe  extends App {

  val data =
    Seq("1183 Amsterdam", "06123 Ankara", "08067 Barcelona", "3030 Bern", "75116 Paris")


  System.setProperty("hadoop.home.dir", "C:/winutils");
  //val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  val spark = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");
  val sparksession: SparkSession = SparkSession.builder.
    master("local").config(spark).getOrCreate()

  import sparksession.sqlContext.implicits._
  val df_schema = StructType(Array(StructField("tobeExtracted", StringType)))
  val cols = Seq("tobeExtracted")
  val df: DataFrame = data.toDF("tobeExtracted")
  df.show(false)
  val rdd= df.rdd
   val finalDf= sparksession.createDataFrame(rdd,df_schema)
  finalDf.show(false)

  import java.util.regex.Pattern

  val whitespace: Pattern = Pattern.compile("\\s")

  val regex1: Regex = "^(.*?)\\s(\\w*?)$".r
  val rgg2= "__________".r
  val reg= "^(?!.*  )[a-zA-Z0-9 ]*$".r
  val regex = "[~!@#$^%&*\\(\\)_+={}\\[\\]|;:\"'<,>.?`/\\\\]".r

  //def remove_leading(regex: String): String => String = _.replaceAll(regex.toString(), "")
  //def extract(regex: String): String => (Array[String]) = _.split(regex)
 val schema = StructType( Array(StructField("postal_code", StringType),StructField("city", StringType)))

  //val extractUDF = udf(extractfeilds(regex),schema)
//extracted.*

  val perfect=
    finalDf.withColumn("postalCode", regexp_extract(col("tobeExtracted"),regex1.toString,1)).
            withColumn("city", regexp_extract(col("tobeExtracted"),regex1.toString,2))
  perfect.show(false)

  val y = Seq(("100-200"),("300-400"),("500-600")).toDF("numbersData").
    withColumn("extractedData",regexp_extract($"numbersData","(\\d  +)-(\\d+)",1)).show(false)
}