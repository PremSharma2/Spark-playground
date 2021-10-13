package part3typesdatasets
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
object DataSetTransformationFunctionally extends App{

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val records = Seq(
    ("x", 4, 1),
    ("x", 6, 2),
    ("z", 7, 3),
    ("a", 3, 4),
    ("z", 5, 2),
    ("x", 7, 3),
    ("x", 9, 7),
    ("z", 1, 8),
    ("z", 4, 9),
    ("z", 7, 4),
    ("a", 8, 5),
    ("a", 5, 2),
    ("a", 3, 8),
    ("x", 2, 7),
    ("z", 1, 9)
  )
  val initial_df = spark.createDataFrame(records).toDF("col1", "col2", "col3")
  initial_df.show(false)
  def inc(i: Int) = i + 1
  def func0(x: Int => Int, y: Int)(in: DataFrame): DataFrame = {
    in.filter(col("col2") > x(y))
  }
  def func1(x: Int)(in: DataFrame): DataFrame = {
    in.selectExpr("col1", "col2","col3",s"col2 + $x as col4")
  }
  def func2(add: Int)(in: DataFrame): DataFrame = {
    in.withColumn("col5", expr(s"col2 + $add"))
  }
  val res = initial_df.transform(func0(inc, 4))
    .transform(func1(1))
    .transform(func2(2))
    .withColumn("col6", expr("col2 + 3"))
  res.show(false)


  /*
TODO
  Another alternative is to define a Scala implicit class,
  which allows you to eliminate the DataFrame transform API:
  */
  implicit class MyTransforms(df: DataFrame) {
    def func0(x: Int => Int, y: Int): DataFrame = {
      df.filter(col("col2") > x(y))
    }
    def func1(x: Int): DataFrame = {
      df.selectExpr("col1", "col2","col3",s"col2 + $x as col4")
    }
    def func2(add: Int): DataFrame = {
      df.withColumn("col5", expr(s"col2 + $add"))
    }
  }
  //Then you can call the functions directly:
  val result1 = initial_df.func0(inc, 1)
    .func1(2)
    .func2(3)
    .withColumn("col6", expr("col2 + 3"))
  result1.show(false)
}
