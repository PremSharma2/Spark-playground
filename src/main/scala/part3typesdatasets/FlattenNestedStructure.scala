package part3typesdatasets
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}
object FlattenNestedStructure  extends App{
  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()
  val structureData = Seq(
    Row(Row("James ","","Smith"),Row(Row("CA","Los Angles"),Row("CA","Sandiago"))),
    Row(Row("Michael ","Rose",""),Row(Row("NY","New York"),Row("NJ","Newark"))),
    Row(Row("Robert ","","Williams"),Row(Row("DE","Newark"),Row("CA","Las Vegas"))),
    Row(Row("Maria ","Anne","Jones"),Row(Row("PA","Harrisburg"),Row("CA","Sandiago"))),
    Row(Row("Jen","Mary","Brown"),Row(Row("CA","Los Angles"),Row("NJ","Newark")))
  )

  val structureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("address",new StructType()
      .add("current",new StructType()
        .add("state",StringType)
        .add("city",StringType))
      .add("previous",new StructType()
        .add("state",StringType)
        .add("city",StringType)))

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(structureData),structureSchema)
  df.printSchema()
  /*
 root
 |-- name: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- middlename: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- address: struct (nullable = true)
 |    |-- current: struct (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |-- previous: struct (nullable = true)
 |    |    |-- state: string (nullable = true)
 |    |    |-- city: string (nullable = true)
   */

  df.show(false)
  /*
  +---------------------+----------------------------------+
|name                 |address                           |
+---------------------+----------------------------------+
|[James , , Smith]    |[[CA, Los Angles], [CA, Sandiago]]|
|[Michael , Rose, ]   |[[NY, New York], [NJ, Newark]]    |
|[Robert , , Williams]|[[DE, Newark], [CA, Las Vegas]]   |
|[Maria , Anne, Jones]|[[PA, Harrisburg], [CA, Sandiago]]|
|[Jen, Mary, Brown]   |[[CA, Los Angles], [NJ, Newark]]  |
+---------------------+----------------------------------+

   */
  val df2 = df.select(col("name.*"),
    col("address.current.*"),
    col("address.previous.*"))
  val df2Flatten = df2.toDF("fname","mename","lname","currAddState",
    "currAddCity","prevAddState","prevAddCity")
  df2Flatten.printSchema()
  df2Flatten.show(false)
  /*
  root
 |-- name_firstname: string (nullable = true)
 |-- name_middlename: string (nullable = true)
 |-- name_lastname: string (nullable = true)
 |-- address_current_state: string (nullable = true)
 |-- address_current_city: string (nullable = true)
 |-- address_previous_state: string (nullable = true)
 |-- address_previous_city: string (nullable = true)
   */

  def flattenStructSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName)
        case _ => Array(col(columnName).as(columnName.replace(".","_")))
      }
    })
  }
  val df3 = df.select(flattenStructSchema(df.schema):_*)
  df3.printSchema()
  df3.show(false)
}
