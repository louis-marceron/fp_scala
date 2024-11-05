package fr.umontpellier.ig5
import org.apache.spark.sql.SparkSession

// https://drive.google.com/drive/folders/12nE-4ffAP9RZD7VkEIY4Ett_9KTKyhv5

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LineCounter")
      .master("local[*]")
      .getOrCreate()

    val filePath = "data/README.md"
    try {
      val lineCount = countLines(spark, filePath)
      println(s"Number of lines in $filePath: $lineCount")
    } catch {
      case e: Exception => println(s"An error occurred: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }

  private def countLines(spark: SparkSession, filePath: String): Long = {
    // Lecture du fichier en tant que RDD
    val lines = spark.sparkContext.textFile(filePath)
    lines.count()  // Compte le nombre de lignes
  }
}
