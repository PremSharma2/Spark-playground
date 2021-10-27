package part2dataframes

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr, max}

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // inner joins
  val joinConditionExpression: Column = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinConditionExpression, "inner")

  // outer joins
  //todo: -> left outer = everything in the inner join + all the rows in the LEFT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinConditionExpression, "left_outer")

  // TOdo : ->right outer = everything in the inner join + all the rows in the RIGHT table, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinConditionExpression, "right_outer")

  // todo : -> outer join = everything in the inner join + all the rows in BOTH tables, with nulls in where the data is missing
  guitaristsDF.join(bandsDF, joinConditionExpression, "outer")

  //todo: -> semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinConditionExpression, "left_semi")

  //todo:-> anti-joins = everything in the left DF for which there is NO row in the right DF satisfying the condition
  //TODO : actually this gives missing Rows in Left DataFrame
  guitaristsDF.join(bandsDF, joinConditionExpression, "left_anti")


  // things to bear in mind
  // guitaristsBandsDF.select("id", "band").show // this crashes

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // TODO : ->using complex types i.e using arrays
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"),
    expr("array_contains(guitars, guitarId)")).show(false)

  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")
  titlesDF.show(false)

  //TODO :-> Exercise: 1  show all employees and their max salary
  val maxSalariesPerEmpNoDF: DataFrame = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  //TODO joining the maxSalariesPerEmpNoDF with employeesDF
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")

  // TODO :Exercise:2
  //TODO  show all employees who were never managers
  val empNeverManagersDF = employeesDF.join(
    deptManagersDF,
    employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
    "left_anti"
  )

  // TODO Exercise :->3 find the job titles of the best paid 10 employees in the company
  //todo making sure we are getting the latest toDate from title DF
  /*
TODO
 +------+----------------+----------+----------+
 |emp_no|title           |from_date |to_date   |
 +------+----------------+----------+----------+
 |10010 |Engineer        |1996-11-24|9999-01-01|
|10020 |Engineer        |1997-12-30|9999-01-01|
|10030 |Engineer        |1994-02-17|2001-02-17|
|10030 |Senior Engineer |2001-02-17|9999-01-01|
|10040 |Engineer        |1993-02-14|1999-02-14|
|10040 |Senior Engineer |1999-02-14|9999-01-01|
|10050 |Senior Staff    |1999-12-25|9999-01-01|
|10050 |Staff           |1990-12-25|1999-12-25|
|10060 |Senior Staff    |1996-05-28|9999-01-01|
|10060 |Staff           |1989-05-28|1996-05-28|
|10070 |Technique Leader|1985-10-14|9999-01-01|
|10080 |Senior Staff    |2001-09-28|9999-01-01|
|10080 |Staff           |1994-09-28|2001-09-28|
|10090 |Senior Engineer |1986-03-14|1999-05-07|
|10100 |Senior Staff    |1994-09-21|9999-01-01|
|10100 |Staff           |1987-09-21|1994-09-21|
|10110 |Senior Staff    |1992-08-21|9999-01-01|
|10110 |Staff           |1986-08-22|1992-08-21|
|10120 |Engineer        |1996-07-06|9999-01-01|
|10130 |Senior Engineer |1988-06-21|9999-01-01|
+------+----------------+----------+----------+
only showing top 20 rows

   */
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title")
    .agg(max("to_date").as("latest-Date"))

  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")

  bestPaidJobsDF.show()
}

