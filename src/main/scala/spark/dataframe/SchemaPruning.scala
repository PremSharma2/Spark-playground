package spark.dataframe


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object SchemaPruning {

  import org.apache.spark.sql.types._

  def pruneNestedColumns(df: DataFrame, columnsToPrune: List[String]): (DataFrame, StructType) = {
    val schema = df.schema


    val prunedFields: Array[StructField] = schema.
      fields.filter(field => columnsToPrune.contains(field.name))

    val prunedSchema: StructType = StructType(prunedFields.map(field => {
      val fieldType = field.dataType match {
        case s: StructType => pruneStructType(s, columnsToPrune)
        case a: ArrayType => pruneArrayType(a, columnsToPrune)
        case other => other
      }
      StructField(field.name, fieldType, field.nullable, field.metadata)
    }))
    val prunedDF = df.select(prunedFields.map(field => col(field.name)): _*)
    (prunedDF, prunedSchema)
  }

  def pruneStructType(structType: StructType, columnsToPrune: List[String]): StructType = {
    val prunedFields = structType.fields.filter(field => columnsToPrune.contains(field.name))
    StructType(prunedFields.map(field => {
      val fieldType = field.dataType match {
        case s: StructType => pruneStructType(s, columnsToPrune)
        case a: ArrayType => pruneArrayType(a, columnsToPrune)
        case other => other
      }
      StructField(field.name, fieldType, field.nullable, field.metadata)
    }))
  }

  def pruneArrayType(arrayType: ArrayType, columnsToPrune: List[String]): ArrayType = {
    val elementType = arrayType.elementType match {
      case s: StructType => pruneStructType(s, columnsToPrune)
      case a: ArrayType => pruneArrayType(a, columnsToPrune)
      case other => other
    }
    ArrayType(elementType, arrayType.containsNull)
  }

//  val spark: SparkSession = createSparkSession
//  val df = spark.read.format("csv").option("header", "true").load("path/to/csv")
//  val columnsToPrune = List("nestedColumn1", "nestedColumn2.nestedColumn3")
//  val (prunedDF, prunedSchema) = pruneNestedColumns(df, columnsToPrune)
//  val prunedDF2 = spark.read.schema(prunedSchema).format("csv").option("header", "true").load("path/to/csv")
  def main(args: Array[String]): Unit = {

//    val df = spark.read.format("csv").option("header", "true").load("path/to/csv")
//   val columnsToPrune = List("nestedColumn1", "nestedColumn2.nestedColumn3")
//  val (prunedDF, prunedSchema) = pruneNestedColumns(df, columnsToPrune)
//   val prunedDF2 = spark.read.schema(prunedSchema).format("csv").option("header", "true").load("path/to/csv")
//   val spark = SparkSession.builder().master("local").getOrCreate()

    val data = Seq(
      (1, "John", Seq(25, 26), (100, "123 Main St", "San Francisco")),
      (2, "Jane", Seq(27, 28), (200, "456 Second St", "Los Angeles"))
    )
    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, true),
      StructField("ages", ArrayType(IntegerType), true),
      StructField("address", StructType(Seq(
        StructField("number", IntegerType, false),
        StructField("street", StringType, true),
        StructField("city", StringType, true))))))

    /*
   TODO
      val schema = StructType(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("address", ArrayType(StructField(StringType)),
      StructField("location", StructType(
      StructField("city", StringType),
      StructField("country", StringType)
    ))
    )
       */
  }
}
