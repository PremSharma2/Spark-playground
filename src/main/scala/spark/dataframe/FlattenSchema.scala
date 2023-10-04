package spark.dataframe


import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}

object FlattenSchema  extends App {

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

  def flattenStructSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName)
        case _ => Array(col(columnName).as(columnName.replace(".",".")))
      }
    })
  }
  val df3 = df.select(flattenStructSchema(df.schema):_*)
  df3.printSchema()
  df3.show(false)
}
