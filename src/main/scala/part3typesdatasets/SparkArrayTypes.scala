package part3typesdatasets
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
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
  val initial_df = spark.createDataFrame(records).toDF("col1", "col2", "col3")
  // Generate Array columns
  val arrayExpression1: Column = collect_list(col("col2")).as("arrayColumn1")
  val arrayExpression2: Column = collect_list(col("col3")).as("arrayColumn2")
  val full_df = initial_df.groupBy("col1").agg(arrayExpression1, arrayExpression2)
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

  /*
TODO
  array_position
 This function returns the position of first occurrence of a specified element. If the element is not present it returns 0.
 Let’s try to find the position of element say ‘7’ from column array_col2 .
  +----+------------------+------+
|col1|        array_col2|result|
+----+------------------+------+
|   x|   [1, 2, 3, 7, 7]|     4|
|   z|[3, 2, 8, 9, 4, 9]|     0|
|   a|      [4, 5, 2, 8]|     0|
+----+------------------+------+
   */
  val arr_pos_df = df.withColumn("result", array_position(col("arrayColumn2"), 7))

  arr_pos_df.show()

  /*
TODO
    array_remove
 This function removes all the occurrences of an element from an array.
 Let’s remove the element ‘7’ from column array_col2.
  +----+------------------+------------------+
|col1|        array_col2|            result|
+----+------------------+------------------+
|   x|   [1, 2, 3, 7, 7]|         [1, 2, 3]|
|   z|[3, 2, 8, 9, 4, 9]|[3, 2, 8, 9, 4, 9]|
|   a|      [4, 5, 2, 8]|      [4, 5, 2, 8]|
+----+------------------+------------------+
 All occurrences of element ‘7’ are removed from array.
   */

  val arr_remove_df = df.withColumn("result", array_remove(col("arrayColumn2"), 7))

  arr_remove_df.show()
  /*
TODO
   array_repeat
 This function creates an array that is repeated as specified by second argument.
   */
  val arr_repeat_df = df.withColumn("result", array_repeat(col("arrayColumn2"), 2))

  arr_repeat_df.show(truncate = false)

  /*
TODO
  array_sort
 This function sorts the elements of an array in ascending order.
 +----+------------------+------------------+
|col1|        array_col2|            result|
+----+------------------+------------------+
|   x|   [1, 2, 3, 7, 7]|   [1, 2, 3, 7, 7]|
|   z|[3, 2, 8, 9, 4, 9]|[2, 3, 4, 8, 9, 9]|
|   a|      [4, 5, 2, 8]|      [2, 4, 5, 8]|
+----+------------------+------------------+
   */
  val arr_sort_df = df.withColumn("result", array_sort(col("arrayColumn2")))

  arr_sort_df.show()

  /*
TODO
 array_union
 This function returns the union of all elements from the input arrays.
 Column result contains the union of arrays from column array_col1 and array_col2 and contains distinct values only.
+------------------+------------------+------------------------+
|[4, 6, 7, 9, 2]   |[1, 2, 3, 7, 7]   |[4, 6, 7, 9, 2, 1, 3]   |
|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|[7, 5, 1, 4, 3, 2, 8, 9]|
|[3, 8, 5, 3]      |[4, 5, 2, 8]      |[3, 8, 5, 4, 2]         |
+------------------+------------------+------------------------+
   */

  val arr_union_df = full_df
    .withColumn("result", array_union(col("arrayColumn1"), col("arrayColumn2")))
    .drop("col1")

  arr_union_df.show(truncate = false)

  /*
TODO
   arrays_overlap
  This function checks if at least one element is common/overlapping in arrays.
  It returns true if at least one element is common in both array and false otherwise.
  It returns null if at least one of the arrays is null.
  All the values in result column are true because
  we have at-least one element common in array_col1 and array_col2 for all rows.
 For example, in the first row the result column is true
 because the elements ‘2’ and ‘7’ are present in both columns array_col1 and array_col2.
  +----+------------------+------------------+------+
|col1|array_col1        |array_col2        |result|
+----+------------------+------------------+------+
|x   |[4, 6, 7, 9, 2]   |[1, 2, 3, 7, 7]   |true  |
|z   |[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|true  |
|a   |[3, 8, 5, 3]      |[4, 5, 2, 8]      |true  |
+----+------------------+------------------+------+
   */
  val arr_overlap_df = full_df
    .withColumn("result", arrays_overlap(col("arrayColumn1"), col("arrayColumn2")))

  arr_overlap_df.show()

  /*
TODO
  arrays_zip
 This function merges the i-th element of an array and returns array<struct>.
 Since both the array columns have same numbers of values,
 let’s remove some values from one array column and see
 how it behaves with different values in the array with zip operation.
 First, we will remove element ‘2’ from array column array_col2
 and then try to zip column array_col1 with newly created column new_array_col

 +------------------+---------------+-------------------------------+
|array_col1        |new_array_col  |result                         |
+------------------+---------------+-------------------------------+
|[4, 6, 7, 9, 2]   |[1, 3, 7, 7]   |[[4, 1], [6, 3] ..., [2,]]     |
|[7, 5, 1, 4, 7, 1]|[3, 8, 9, 4, 9]|[[7, 3], [5, 8] ..., [1,]]     |
|[3, 8, 5, 3]      |[4, 5, 8]      |[[3, 4], ... [3,]]             |
+------------------+---------------+-------------------------------+
In first row,
first element of result column is [4, 1] which is a zip of first element from array array_col1 (4)
and new_array_col (1).
Also, last element of result column is [2,] (which is a zip of 5-th element)
and second value is blank because there is no 5-th element in first row of column new_array_col.
   */
  // remove element "2" from array column "array_col2"
  val temp_df = full_df
    .withColumn("new_array_col", array_remove(col("arrayColumn2"), 2))

  // zip column "array_col1" with newly created column "new_array_col"
  val arr_zip_df = temp_df
    .withColumn("result", arrays_zip(col("arrayColumn1"), col("arrayColumn2")))
    .select("arrayColumn1", "new_array_col", "result")

  arr_zip_df.show(truncate = false)
  arr_zip_df.select("result").printSchema()

  /*
TODO
  concat
 This function concatenates all the elements of both arrays into a single one.
+------------------+------------------+----------------------------+
|array_col1        |array_col2        |result                      |
+------------------+------------------+----------------------------+
|[4, 6, 7, 9, 2]   |[1, 2, 3, 7, 7]   |[4, 6, 7, 9, ..., 3, 7, 7]  |
|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|[7, 5, 1, 4, ..., 9, 4, 9]  |
|[3, 8, 5, 3]      |[4, 5, 2, 8]      |[3, 8, 5, 3, 4, 5, 2, 8]    |
+------------------+------------------+----------------------------+
   */

  val arr_cat_df = full_df.withColumn("result", concat(col("arrayColumn1"), col("arrayColumn2")))

  arr_cat_df.show(truncate = false)

  /*
TODO
  element_at
 This function returns the element at a specified index.
 Let’s try to get the first element from each array.
 Column result contains the first element from each array.
 For example, in the first row the result contains ‘1’
 because this is first element in the array [1, 2, 3, 7, 7].
 +----+------------------+------+
|col1|array_col2        |result|
+----+------------------+------+
|x   |[1, 2, 3, 7, 7]   |1     |
|z   |[3, 2, 8, 9, 4, 9]|3     |
|a   |[4, 5, 2, 8]      |4     |
+----+------------------+------+
   */
  val arr_element_at_df = df.withColumn("result", element_at(col("arrayColumn2"), 1))

  arr_element_at_df.show()
  /*
TODO
 flatten
 This function returns a single array from array of an arrays.
 If an array is more than 2 levels deep,
 it removes one level of nesting from an array.
 Let’s first generate the nested array using the function array_repeat
 as discussed above and then flatten the nested array.
 +-----------------------------------+------------------------------+
|repeat                             |result                        |
+-----------------------------------+------------------------------+
|[[1, 2, 3, 7, 7], [1, 2, 3, 7, 7]] |[1, 2, 3, 7, 7, 1, 2, 3, 7, 7]|
|[[3, 2, 8, 9, 4], [3, 2, 8, 9, 4]] |[3, 2, 8, 9, 4, 3, 2, 8, 9, 4]|
|[[4, 5, 2, 8], [4, 5, 2, 8]]       |[4, 5, 2, 8, 4, 5, 2, 8]      |
+----------------------------------------+-------------------------+
Column result contains all the values from an array of arrays from column repeat but in a single array.
   */

  val arr_repeat_df1 = df.withColumn("repeat", array_repeat(col("arrayColumn2"), 2))

  // flatten the nested array.
  val arr_flat_df = arr_repeat_df1
    .withColumn("result", flatten(col("repeat")))
    .select("repeat", "result")

  arr_flat_df.show(truncate = false)
  /*
 TODO
   map_from_arrays
  This function creates a map column.
  Elements of the first column will be used
  for keys and second column will be used for values.

+------------------+------------------+--------------------+
|        array_col1|        array_col2|              result|
+------------------+------------------+--------------------+
|   [4, 6, 7, 9, 2]|   [1, 2, 3, 7, 7]|[4 -> 1, 6 -> 2, ...|
|[7, 5, 1, 4, 7, 1]|[3, 2, 8, 9, 4, 9]|[7 -> 3, 5 -> 2, ...|
|      [3, 8, 5, 3]|      [4, 5, 2, 8]|[3 -> 4, 8 -> 5, ...|
+------------------+------------------+--------------------+
Column result contains the Map generated from both the input arrays.
The first element in first row is 4 -> 1 where ‘4’ is a key
which is the first element from first column array_col1 and ‘1’
is the key’s value which is the first element from second column array_col2.
   */


  val map_from_arr_df = full_df
    .withColumn("result", map_from_arrays(col("arrayColumn1"), col("arrayColumn2")))
    .drop("col1")

  map_from_arr_df.show(truncate = false)
  /*
TODO
 reverse
 This function reverses the order of elements in input array.
 +----+------------------+------------------+
|col1|        array_col2|            result|
+----+------------------+------------------+
|   x|   [1, 2, 3, 7, 7]|   [7, 7, 3, 2, 1]|
|   z|[3, 2, 8, 9, 4, 9]|[9, 4, 9, 8, 2, 3]|
|   a|      [4, 5, 2, 8]|      [8, 2, 5, 4]|
+----+------------------+------------------+
   */

  val arr_reverse_df = df.withColumn("result", reverse(col("arrayColumn2")))

  arr_reverse_df.show()

  /*
TODO
   shuffle
 This function shuffles the elements of array randomly.
+----+------------------+------------------+
|col1|        array_col2|            result|
+----+------------------+------------------+
|   x|   [1, 2, 3, 7, 7]|   [2, 7, 1, 7, 3]|
|   z|[3, 2, 8, 9, 4, 9]|[3, 8, 9, 4, 9, 2]|
|   a|      [4, 5, 2, 8]|      [8, 4, 2, 5]|
+----+------------------+------------------+
Column result contains shuffled elements from column array_col2.
In other words, order of elements in result column is random.
For example, in the first row the result column contains [2, 7, 1, 7, 3]
which is the shuffled output of array [1, 2, 3, 7, 7] from column array_col2.
   */
  val arr_shuffle_df1 = df.withColumn("result", shuffle(col("arrayColumn2")))

  arr_shuffle_df1.show()
  /*
TODO
  size
 This function returns a number of elements in an array or map.
 +----+------------------+------+
|col1|        array_col2|result|
+----+------------------+------+
|   x|   [1, 2, 3, 7, 7]|     5|
|   z|[3, 2, 8, 9, 4, 9]|     6|
|   a|      [4, 5, 2, 8]|     4|
+----+------------------+------+
  */

  val arr_size_df = df.withColumn("result", size(col("arrayColumn2")))

  arr_size_df.show()

  /*
TODO
  slice
 This function slices the array into a sub-array.
 We can specify the start of the index as second argument
 and number of elements as third argument.
 Note: Arrays in spark start with index 1.
 It also supports negative indexing to access the elements from last.
 Let’s try to create a sub-array of 3 elements starting from index 2.
 +----+------------------+---------+
|col1|        array_col2|   result|
+----+------------------+---------+
|   x|   [1, 2, 3, 7, 7]|[2, 3, 7]|
|   z|[3, 2, 8, 9, 4, 9]|[2, 8, 9]|
|   a|      [4, 5, 2, 8]|[5, 2, 8]|
+----+------------------+---------+
TODO
 In first row, result contains sub-array [2, 3, 7]
 which is created with 3 elements from index 2 in [1, 2, 3, 7, 7].
   */
  val arr_slice_df = df.withColumn("result", slice(col("arrayColumn2"), 2, 3))

  arr_slice_df.show()


 /*
TODO
 explode
 This function creates a new row for each element of an array or map.
 Let’s first create new column with fewer values to explode.
 +----+---------+
|col1|slice_col|
+----+---------+
|   x|   [1, 2]|
|   z|   [3, 2]|
|   a|   [4, 5]|
+----+---------+
slice_col contains 2 elements in an array.
So upon explode, this generates 2 rows for each array.

+----+---------+------+
|col1|slice_col|result|
+----+---------+------+
|   x|   [1, 2]|     1|
|   x|   [1, 2]|     2|
|   z|   [3, 2]|     3|
|   z|   [3, 2]|     2|
|   a|   [4, 5]|     4|
|   a|   [4, 5]|     5|
+----+---------+------+
Upon explode, 2 rows are generated for each element of an array in column slice_col.

  */
 // create an array column with few values to explode.
 val temp_df1 = df.withColumn("slice_col", slice(col("arrayColumn2"), 1, 2))
   .drop("array_col2")

  temp_df1.show()

  // explode the array column "slice_col"
  val arr_explode_df = temp_df1.withColumn("result", explode(col("slice_col")))

  arr_explode_df.show()
}

