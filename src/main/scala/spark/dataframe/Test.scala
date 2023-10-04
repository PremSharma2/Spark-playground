package spark.dataframe


import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
object Test  extends App {

  System.setProperty("hadoop.home.dir", "C:/winutils");
  //val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  val spark = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");
  val sparksession: SparkSession = SparkSession.builder.master("local").config(spark).getOrCreate()
  import sparksession.sqlContext.implicits._

case class Container( dataframe:Dataset[_])
  val studentsDF = Seq(
    ("mario", "italy", "europe"),
    ("stefano", "italy", "europe"),
    ("victor", "spain", "europe"),
    ("li", "china", "asia"),
    ("yuki", "japan", "asia"),
    ("vito", "italy", "europe")
  ).toDF("name", "country", "continent")

  val studentsDF1 = Seq(
    ("mario", "italy", "europe"),
    ("stefano", "italy", "europe"),
    ("victor", "spain", "europe"),
    ("li", "china", "asia"),
    ("yuki", "japan", "asia"),
    ("vito", "italy", "europe")
  ).toDF("name", "country", "continent")

  studentsDF
    .groupBy("continent", "country")

val first :: second :: Nil = List(studentsDF,studentsDF1)

  val diffrence: Dataset[Row] = first.except(second)
  val containerList=List(Container(studentsDF1),Container(studentsDF))
  val fs :: second1 :: Nil = containerList.map(_.dataframe.toDF)
}
