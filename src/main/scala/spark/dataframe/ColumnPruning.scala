package spark.dataframe


import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object ColumnPruning {

  import org.apache.spark.sql.DataFrame

  def pruneColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
    // Get the columns to keep
    val keepColumns: Array[String] = df.columns.filter(columns.contains)

    // Prune the columns
    val prunedDF = df.selectExpr(keepColumns: _*)

    // Update the schema for nested structures
    val updatedSchema = prunedDF.schema.fields.map(field => {
      if (field.dataType.typeName == "struct") {
        val newDataType = DataTypes.createStructType(pruneNestedColumns(field.dataType.asInstanceOf[StructType], columns))
        StructField(field.name, newDataType, field.nullable)
      } else {
        field
      }
    })

    // Create a new DataFrame with the updated schema
    prunedDF.sqlContext.createDataFrame(prunedDF.rdd, StructType(updatedSchema))
  }

  def pruneNestedColumns(structType: StructType, columns: Seq[String]): Array[StructField] = {
    structType.fields.map(field => {
      if (columns.contains(field.name)) {
        field
      } else if (field.dataType.typeName == "struct") {
        StructField(field.name, DataTypes.createStructType(pruneNestedColumns(field.dataType.asInstanceOf[StructType], columns)), field.nullable)
      } else {
        null
      }
    }).filter(_ != null)
  }


 // # Create a SparkSession
  val spark = SparkSession.builder.appName("ColumnPruningExample").getOrCreate()

  //# Define a sample DataFrame with nested structures
    val data = Seq(Row("Alice", 25, ("London", "UK")), Row("Bob", 30, ("New York", "USA")))
/*

  val arrayArraySchema = new StructType()
    .add("name",StringType)
    .add("age",IntegerType)
    .add("location",ArrayType(ArrayType(StringType)))

  val schema = StructType(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("location", StructType(
    StructField("city", StringType),
    StructField("country", StringType)
  ))
  )
 //val df = spark.createDataFrame(data, schema)


/*
    sql
  Copy code
    +-----+------------+
  | name|        city|
  +-----+------------+
  |Alice|      London|
  |  Bob|    New York|
    +-----+------------+



 */



  Regenerate response


 */
}
