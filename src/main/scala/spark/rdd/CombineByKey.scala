package spark.rdd


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object CombineByKey extends App {
  System.setProperty("hadoop.home.dir", "C:/winutils");
  val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  val sparksession = createSparkSession

  private def createSparkSession: SparkSession = {
    val spark = new SparkConf().setAppName("Simple Application").
      setMaster(("local[*]")).
      set("spark.executor.memory", "1g") // 4 workers
      .set("spark.executor.instances", "1")
      // 5 cores on each workers
      .set("spark.executor.cores", "5");
    SparkSession.builder.master("local").config(spark).getOrCreate()
  }

  val sparkContext = sparksession.sparkContext
  val studentRDD = sparkContext.parallelize(Array(
    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91),
    ("Joseph", "Biology", 82), ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62),
    ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80), ("Tina", "Maths", 78),
    ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91),
    ("Thomas", "Biology", 74), ("Cory", "Maths", 56), ("Cory", "Physics", 65),
    ("Cory", "Chemistry", 71), ("Cory", "Biology", 68), ("Jackeline", "Maths", 86),
    ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64),
    ("Juan", "Biology", 60)), 3)

  //Defining createCombiner, mergeValue and mergeCombiner functions
  def createCombiner = (tuple: (String, Int)) =>
    (tuple._2.toDouble, 1)

  def mergeValue = (accumulator: (Double, Int), element: (String, Int)) =>
    (accumulator._1 + element._2, accumulator._2 + 1)

  def mergeCombiner = (accumulator1: (Double, Int), accumulator2: (Double, Int)) =>
    (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)


  // use combineByKey for finding percentage
  val combRDD = studentRDD.map(t => (t._1, (t._2, t._3)))
    .combineByKey(createCombiner, mergeValue, mergeCombiner)
    .map(e => (e._1, e._2._1/e._2._2))

  //Check the Outout
  combRDD.collect foreach println

  // Output
  // (Tina,76.5)
  // (Thomas,86.25)
  // (Jackeline,76.5)
  // (Joseph,82.5)
  // (Juan,64.0)
  // (Jimmy,77.0)
  // (Cory,65.0)
}