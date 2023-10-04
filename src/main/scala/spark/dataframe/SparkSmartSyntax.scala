package spark.dataframe


import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}

object SparkSmartSyntax  extends App {

  import org.apache.spark.sql.functions.col

  System.setProperty("hadoop.home.dir", "C:/winutils");
  //val logFile = "D:/Spark_VM/emp.txt" // Should be some file on your system
  val spark = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");
  val sparksession: SparkSession = SparkSession.builder.master("local").config(spark).getOrCreate()
  val data = List(("James ","","Smith","36636","M",60000),
    ("Michael ","Rose",null ,"40288","M",70000),
    ("Robert ",null,"Williams","42114","",400000),
    ("Maria ","Anne","Jones","39192","F",500000),
    ("Jen","Mary","Brown","39192","F",0))

  val cols = Seq("first_name","middle_name","last_name","dob","gender","salary")
  val df= sparksession.createDataFrame(data).toDF(cols:_*)
  val columns = Seq[String]("col1", "col2", "col3")
  val colNames: Seq[Column] = columns.map(name => col(name))
  val df1 = df.select(colNames:_*)

  df.select(cols.head,cols.tail: _*)

}
