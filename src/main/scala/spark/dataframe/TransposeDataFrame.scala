package spark.dataframe


import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
object TransposeDataFrame  extends App {
  import org.apache.spark.sql.functions.{col, lit, when}

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
  val df = Seq(("prem","maths",20),("amit","physics",50),("amit","maths",80)).toDF("name","subject","marks")
/*
+----+-------+-----+
|name|subject|marks|
+----+-------+-----+
|prem|  maths|   20|
|amit|physics|   50|
|amit|  maths|   80|
+----+-------+-----+
 */
  val pivot_df = df.groupBy("name").
                    pivot("subject").
                   max("marks")
  pivot_df.show(false)
/*
----+-----+-------+
|name|maths|physics|
+----+-----+-------+
|amit|80   |50     |
|prem|20   |null   |
+----+-----+-------+
 */

  /*
  17

If you use the selectfunction on a dataframe you get a dataframe back.
Then you apply a function on the Rowdatatype not the value of the row.
 Afterwards you should get the value first so you should do the following:

df.select("start").map(el->el.getString(0)+"asd")

But you will get an DataSet[String] as return value not a DF
   */
  val cols: Seq[String] = df.select("subject").
    distinct.
    map(r => r.getString(0)).collect()
/*
val df4 = df.select(col("*"),
 when(col("gender") === "M","Male")
 .when(col("gender") === "F","Female") .otherwise("Unknown").alias("new_gender"))

 or
 al newDf = df.withColumn("D", when($"B".isNull or $"B" === "", 0).otherwise(1))
 */
  val result: Seq[Column] =cols.map(c => when(col(c).isNull,lit(0))
    .otherwise(col(c)))
  val final_df = pivot_df.withColumn("totalmarks",
     cols.map(c => when(col(c).isNull,lit(0))
    .otherwise(col(c))).reduce(_ + _).as("totalmarks"))

  final_df.show(false)
}
