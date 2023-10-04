package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object simpleApp {
 
  def main(args: Array[String]) {
    
   /* val sparkBuilder: SparkSession.Builder = SparkSession
      .builder
      .appName("CreateModelDataPreparation")
      .master("local")*/
    //implicit val spark: SparkSession = sparkBuilder.config("spark.master", "local").getOrCreate()
    System.setProperty("hadoop.home.dir", "C:/winutils");
    val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
    val spark = new SparkConf().setAppName("Simple Application").setMaster(("local[*]"))
    val sparkContext=new SparkContext(spark)
    val logData=sparkContext.textFile(logFile, 2).saveAsTextFile("user/prem/spark")
   // val logData = spark.read.textFile(logFile).cache()
   // val numAs = logData.filter(line => line.contains("a")).count()
    //val numBs = logData.filter(line => line.contains("b")).count()
    //println(s"Lines with a: $numAs, Lines with b: $numBs")
    sparkContext.stop()
  }
}
