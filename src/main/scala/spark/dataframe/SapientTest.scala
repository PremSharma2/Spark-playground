package spark.dataframe


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object SapientTest extends App {
  import org.apache.spark.SparkConf

  System.setProperty("hadoop.home.dir", "C:/winutils")


  val sparkConf = new SparkConf().setAppName("Simple Application").
    setMaster(("local[*]")).
    set("spark.executor.memory", "1g") // 4 workers
    .set("spark.executor.instances", "1")
    .set("spark.sql.shuffle.partitions","2")
    .set("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse/warehouse2")
    // 5 cores on each workers
    .set("spark.executor.cores", "5");
  val spark = SparkSession
    .builder()
    .appName("Simple Application ")
    .master("local")
    .config(sparkConf)
    .getOrCreate()
  spark.sparkContext.setLogLevel("Error")
  spark.sparkContext.setLogLevel("Error")
  import org.apache.spark.sql.functions._
  import spark.implicits._

  val userActivity: DataFrame = Seq(
    ("2018-01-01 11:00:00", "u1"),
    ("2018-01-01 12:10:00", "u1"),
    ("2018-01-01 13:00:00", "u1"),
    ("2018-01-01 13:50:00", "u1"),
    ("2018-01-01 14:40:00", "u1"),
    ("2018-01-01 15:30:00", "u1"),
    ("2018-01-01 16:20:00", "u1"),
    ("2018-01-01 16:50:00", "u1"),
    ("2018-01-01 11:00:00", "u2"),
    ("2018-01-02 11:00:00", "u2")
  ).toDF("click_time", "user_id")

  def clickSessList(tmo: Long) =
    udf{ (uid: String, clickList: Seq[String], tsList: Seq[Long]) =>
    def sid(n: Long) = s"$uid-$n"

    val sessList = tsList.foldLeft( (List[String](), 0L, 0L) ){ case ((ls, j, k), i) =>
      if (i == 0 || j + i >= tmo) (sid(k + 1) :: ls, 0L, k + 1) else
        (sid(k) :: ls, j + i, k)
    }._1.reverse

    clickList zip sessList
  }

  val tmo1: Long = 60 * 60
  val tmo2: Long = 2 * 60 * 60

  val win1 = Window.partitionBy("user_id").orderBy("click_time")

  val df1 = userActivity.
    withColumn("ts_diff", unix_timestamp($"click_time") - unix_timestamp(
      lag($"click_time", 1).over(win1))
    ).
    withColumn("ts_diff", when(row_number.over(win1) === 1 || $"ts_diff" >= tmo1, 0L).
      otherwise($"ts_diff")
    )

  df1.show

  val df2 = df1.
    groupBy("user_id").agg(
    collect_list($"click_time").as("click_list"), collect_list($"ts_diff").as("ts_list")
  ).
    withColumn("click_sess_id",
      explode(clickSessList(tmo2)($"user_id", $"click_list", $"ts_list"))
    ).
    select($"user_id", $"click_sess_id._1".as("click_time"), $"click_sess_id._2".as("sess_id"))

  df2.show
}
