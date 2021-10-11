package part3typesdatasets
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
object SparkArrayTypes extends App {

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
  val initial_df = spark.createDataFrame(records).toDF("col1","col2","col3")
  // Generate Array columns
  val arrayExpression1: Column = collect_list(col("col2")).as("arrayColumn1")
  val arrayExpression2: Column = collect_list(col("col3")).as("arrayColumn2")
  val full_df= initial_df.groupBy("col1").agg(arrayExpression1,arrayExpression2)
  val df = full_df.drop("arrayColumn1")
  full_df.printSchema()
  /*
TODO
   If we need to find a particular element is present in array,
    we can use array_contains function.
   This function returns true if the value is present in array and false otherwise.
   +----+------------------+------+
|col1|        array_col2|result|
+----+------------------+------+
|   x|   [1, 2, 3, 7, 7]|  true|
|   z|[3, 2, 8, 9, 4, 9]|  true|
|   a|      [4, 5, 2, 8]| false|
+----+------------------+------+
   */
  val arr_contains_df = df.withColumn("result", array_contains(col("arrayColumn2"), 3))

  arr_contains_df.show()

/*
TODO
  This function returns only distinct values from an array and removes duplicate values.
 +----+------------------+---------------+
 |col1|        array_col2|         result|
 +----+------------------+---------------+
 |   x|   [1, 2, 3, 7, 7]|   [1, 2, 3, 7]|
 |   z|[3, 2, 8, 9, 4, 9]|[3, 2, 8, 9, 4]|
 |   a|      [4, 5, 2, 8]|   [4, 5, 2, 8]|
+----+------------------+---------------+
 */
  val arr_distinct_df = df.withColumn("result", array_distinct(col("arrayColumn2")))

  arr_distinct_df.show()

/*
TODO
   This function returns the elements from first array which are not present in second array.
   This is logically equivalent to set subtract operation.
   +----+------------------+------------------+---------+
|col1|        array_col1|        array_col2|   result|
+----+------------------+------------------+---------+
|   x|   [4, 6, 7, 9, 2]|   [1, 2, 3, 7, 7]|[4, 6, 9]|
|   z|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|[7, 5, 1]|
|   a|      [3, 8, 5, 3]|      [4, 5, 2, 8]|      [3]|
+----+------------------+------------------+---------+
 */
  val arr_except_df = full_df.withColumn("result", array_except(col("arrayColumn1"), col("arrayColumn2")))

/*
TODO
  This function returns common elements from both arrays.
  This is logically equivalent to set intersection operation.
  +----+------------------+------------------+------+
|col1|        array_col1|        array_col2|result|
+----+------------------+------------------+------+
|   x|   [4, 6, 7, 9, 2]|   [1, 2, 3, 7, 7]|[7, 2]|
|   z|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|   [4]|
|   a|      [3, 8, 5, 3]|      [4, 5, 2, 8]|[8, 5]|
+----+------------------+------------------+------+
 */
  val arr_intersect_df = full_df
    .withColumn("result", array_intersect(col("arrayColumn1"), col("arrayColumn2")))

/*
TODO
  This Function joins all the array elements based on delimiter defined as the second argument.
  +----+------------------+-----------+
|col1|        array_col2|     result|
+----+------------------+-----------+
|   x|   [1, 2, 3, 7, 7]|  1,2,3,7,7|
|   z|[3, 2, 8, 9, 4, 9]|3,2,8,9,4,9|
|   a|      [4, 5, 2, 8]|    4,5,2,8|
+----+------------------+-----------+
 */
  val arr_join_df = df.withColumn("result", array_join(col("arrayColumn2"), ","))

  arr_join_df.show()


/*
TODO
  This function returns the maximum value from an array.
   +----+------------------+------+
|col1|        array_col2|result|
+----+------------------+------+
|   x|   [1, 2, 3, 7, 7]|     7|
|   z|[3, 2, 8, 9, 4, 9]|     9|
|   a|      [4, 5, 2, 8]|     8|
+----+------------------+------+
 */
  val arr_max_df = df.withColumn("result", array_max(col("arrayColumn2")))
  arr_max_df.show()

/*
TODO
   This function returns the minimum value from an array.
+----+------------------+------+
|col1|        array_col2|result|
+----+------------------+------+
|   x|   [1, 2, 3, 7, 7]|     1|
|   z|[3, 2, 8, 9, 4, 9]|     2|
|   a|      [4, 5, 2, 8]|     2|
+----+------------------+------+
 */
  val arr_min_df = df.withColumn("result", array_min(col("arrayColumn2")))
  arr_min_df.show()
}
