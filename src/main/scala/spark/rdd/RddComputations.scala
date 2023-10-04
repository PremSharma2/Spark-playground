package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.col


object RddComputations extends App {
  /**
   * 
   * basic intrnsic properties of an RDD
   * abstract class RDD[T] {
   * 
   * Used exclusively when RDD computes a partition (possibly by reading from a checkpoint).
   * compute is implemented by any type of RDD in Spark and is called every time the records are requested unless
   *  RDD is cached or checkpointed (and the records can be read from an external storage, but this time closer to the compute node).
  def compute(split: Partition, context: TaskContext): Iterator[T]
  Used exclusively when RDD is requested for its partitions (called only once as the value is cached).
  def getPartitions: Array[Partition]
  Used when RDD is requested for its dependencies (called only once as the value is cached).
  def getDependencies: Seq[Dependency[_]]
  Defines placement preferences of a partition.

Used exclusively when RDD is requested for the preferred locations of a partition.
  def getPreferredLocations(split: Partition): Seq[String] = Nil
  Defines the Partitioner of a RDD
  val partitioner: Option[Partitioner] = None
}
   **/
  System.setProperty("hadoop.home.dir", "C:/winutils");
  val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  // val spark = new SparkConf().setAppName("Simple Application").setMaster(("local[*]"))
  val sparksession: SparkSession = SparkSession.builder.master("local").getOrCreate()
  val sparkContext = sparksession.sparkContext
  //file read from spark
 val empData=sparkContext.textFile(logFile, 2)
 println(empData.collect().size)
 println(empData.collect().foreach(println))
 val emp_header=  empData.first()
 println(emp_header)
 //filter out the header
 val emp_data_without_header=empData.filter(record => !record.equals(emp_header) )
 println(emp_data_without_header.collect().foreach(println))
 println("No.ofpartition="+emp_data_without_header.partitions.size)
 val emp_salary_list=emp_data_without_header.map(data => data.split(',').apply(5).toDouble)
 //println(emp_salary_list.foreach(println))
 println("Highestsalaty:"+emp_salary_list.max())
  println("minimumsalary:"+emp_salary_list.min())
  val min_salary=emp_salary_list.distinct().sortBy(x => x.toDouble,true)
  print(min_salary.take(1).foreach(println))
  val second_highest_salary=emp_salary_list.zipWithIndex().filter(tuple => tuple._2==1)
  
  val salaryWithEmployeeName=emp_data_without_header.map(row => row.split(",")).
                                                     map(arrstring=> (arrstring(1),arrstring(5).toDouble))
                                                     .collectAsMap()
     println(salaryWithEmployeeName.foreach(println))   
     
     /*
      * def maxBy[B](f: ((String, Double)) => B)(implicit cmp: Ordering[B]): (String, Double)

        Finds the first element which yields the largest value measured by function f. 
      * 
      */
     val maxmValuePair=salaryWithEmployeeName.maxBy{
      case (key,value) => value
  }
  
   val maxmpair=salaryWithEmployeeName.maxBy( pair => pair._2)
   println(maxmpair)
  
}