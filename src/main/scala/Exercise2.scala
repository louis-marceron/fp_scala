package fr.umontpellier.ig5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Exercise2 {

  /**
   * Execute exercise 2 :
   * - Filter users aged 25 and above.
   * - Transform data to extract names and cities.
   * - Group users by city.
   *
   * * @param spark    The Spark session.
   * * @param filePath The path to the CSV file.
   */
  def execute(spark: SparkSession, filePath: String): Unit = {
    try {
      // Define the schema of the CSV file
      val schema = StructType(Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("city", StringType, nullable = false),
      ))

      val dataFrame = spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .schema(schema)
        .csv(filePath)

      println("Whole dataframe")
      dataFrame.show()

      println("Users aged 25 and above")
      val usersAged25AndAbove = dataFrame.filter("age > 25")
      usersAged25AndAbove.show()

      val extractedCities = dataFrame.select("city")
      println("Extract names")
      extractedCities.show()

      val extractedNames = dataFrame.select("name")
      println("Extract names")
      extractedNames.show()

      val groupedUsersByCity = dataFrame.groupBy("city").count()
      println("Group users by city")
      groupedUsersByCity.show()
    } catch {
      case e: Exception => println(s"Error while executing Exercise 1: ${e.getMessage}")
    }
  }
}
