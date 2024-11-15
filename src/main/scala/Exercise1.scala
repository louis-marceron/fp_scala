package fr.umontpellier.ig5

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object Exercise1 {

  /**
   * Execute Exercise 1: Read a CSV file and display the row with the highest score.
   *
   * @param spark    The Spark session.
   * @param filePath The path to the CSV file.
   */
  def execute(spark: SparkSession, filePath: String): Unit = {
    try {
      // Define the schema of the CSV file
      val schema = StructType(Array(
        StructField("type", IntegerType, nullable = true),
        StructField("id", IntegerType, nullable = true),
        StructField("unknown1", StringType, nullable = true),
        StructField("unknown2", IntegerType, nullable = true),
        StructField("unknown3", IntegerType, nullable = true), // Assuming this is the score column
        StructField("language", StringType, nullable = true)
      ))

      // Read the CSV file into a DataFrame
      val dataFrame = spark.read
        .option("header", "false")
        .option("delimiter", ",")
        .schema(schema)
        .csv(filePath)

      // Display a sample of the data
      dataFrame.show()

      // Find the row with the highest score
      val highestScoreRow = dataFrame.orderBy(desc("unknown3")).first()
      println(s"Row with the highest score: $highestScoreRow")

    } catch {
      case e: Exception => println(s"Error while executing Exercise 1: ${e.getMessage}")
    }
  }
}