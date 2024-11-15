package fr.umontpellier.ig5

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("SparkExercises")
      .master("local[*]") // Use all available cores
      .getOrCreate()

    val exercise1CSVPath = "data/stackoverflow.csv"
    val exercise2CSVPath = "data/exercise2.csv"

    // Exercise1.execute(spark, exercise1CSVPath)
    Exercise2.execute(spark, exercise2CSVPath)

    spark.stop()
  }
}