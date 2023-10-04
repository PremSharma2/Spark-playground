package spark.dataframe


import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, GivenWhenThen}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers
import spark.dataframe.SchemaPruning._

import scala.collection.JavaConversions._

class PruneNestedColumnsSpec extends AnyFeatureSpec with GivenWhenThen with Matchers with BeforeAndAfterAll {
  System.setProperty("hadoop.home.dir", "C:\\tools\\hadoop")
  private def createSparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Spark Movie ")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")
    spark
  }
  val spark: SparkSession = createSparkSession
  var df: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val data = Seq(
      Row(1, "John", Seq(25, 26), (100, "123 Main St", "San Francisco")),
      Row(2, "Jane", Seq(27, 28), (200, "456 Second St", "Los Angeles"))
    )
    val schema = StructType(Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, true),
      StructField("ages", ArrayType(IntegerType), true),
      StructField("address", StructType(Seq(
        StructField("number", IntegerType, false),
        StructField("street", StringType, true),
        StructField("city", StringType, true))))))

    val input= spark.createDataFrame(data, schema)
    input.write.parquet("C:\\JobOutput\\input.parquet")
    df = spark.read.format("parquet").option("header", "true").load("C:\\JobOutput\\input.parquet")
  }

  Feature("Pruning nested columns from a DataFrame") {


    Scenario("Pruning a single level nested column") {
      Given("a DataFrame with a single level nested column")
      val columnsToPrune = List("address.city")
      val expectedSchema = StructType(Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("address", StructType(Seq(
          StructField("city", StringType, true)
        )), true)
      ))

      When("the nested column is pruned")
      val (prunedDF, prunedSchema) = pruneNestedColumns(df, columnsToPrune)

      Then("the pruned DataFrame should only contain the non-nested columns")
      prunedDF.columns should contain theSameElementsAs Seq("id", "name", "city")

      And("the schema of the pruned DataFrame should match the expected schema")
      prunedSchema shouldEqual expectedSchema
      val prunedDF2 = spark.read.schema(prunedSchema).format("parquet").option("header", "true").load("C:\\JobOutput\\input.parquet")
      prunedDF2.show(false)
    }

    Scenario("Pruning a multi-level nested column") {
      Given("a DataFrame with a multi-level nested column")
      val columnsToPrune = List("nestedColumn2.nestedColumn3.nestedColumn4")
      val expectedSchema = StructType(Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("nestedColumn1", StructType(Seq(
          StructField("nestedColumn1Field1", StringType, true)
        )), true),
        StructField("nestedColumn2", StructType(Seq(
          StructField("nestedColumn2Field1", StringType, true)
        )), true)
      ))

      When("the nested column is pruned")
      val (prunedDF, prunedSchema) = pruneNestedColumns(df, columnsToPrune)

      Then("the pruned DataFrame should only contain the non-nested columns")
      prunedDF.columns should contain theSameElementsAs Seq("id", "name", "nestedColumn1", "nestedColumn2")

      And("the schema of the pruned DataFrame should match the expected schema")
      prunedSchema shouldEqual expectedSchema
    }

    Scenario("Pruning an array of nested structs") {
      Given("a DataFrame with an array of nested structs")
      val columnsToPrune = List("arrayColumn.nestedColumn1.nestedColumn2")
      val expectedSchema = StructType(Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("arrayColumn", ArrayType(StructType(Seq(
          StructField("nestedColumn1", StructType(Seq(
            StructField("nestedColumn1Field1", StringType, true)
          )), true)
        )), true))
      ))
    }
  }}
