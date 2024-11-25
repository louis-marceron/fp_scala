import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("SparkExercises")
      .master("local[*]") // Use all available cores
      .getOrCreate()

    val inputDir = "data/cve_data"

    try {
      // Initialize an empty DataFrame for merging
      var mergedData: DataFrame = spark.emptyDataFrame

      // Loop through the years 2023 to 2024
      for (year <- 2023 to 2024) {
        val filePath = s"$inputDir/nvdcve-1.1-$year.json"
        println(s"Reading file: $filePath")

        // Read the JSON file with multiLine option
        val jsonDF = spark.read.option("multiLine", "true").json(filePath)

        // Extract necessary fields and handle nested structures
        val cveDF = jsonDF
          .select(explode(col("CVE_Items")).as("CVE_Item"))
          .select(
            col("CVE_Item.cve.CVE_data_meta.ID").as("ID"),
            expr("filter(CVE_Item.cve.description.description_data, x -> x.lang = 'en')[0].value").as("Description"),
            expr("filter(CVE_Item.cve.problemtype.problemtype_data[0].description, x -> x.lang = 'en')[0].value").as("ProblemType"),
            coalesce(
              col("CVE_Item.impact.baseMetricV3.cvssV3.baseScore"),
              col("CVE_Item.impact.baseMetricV2.cvssV2.baseScore")
            ).as("BaseScore"),
            coalesce(
              col("CVE_Item.impact.baseMetricV3.cvssV3.baseSeverity"),
              col("CVE_Item.impact.baseMetricV2.cvssV2.severity")
            ).as("Severity"),
            coalesce(
              col("CVE_Item.impact.baseMetricV3.exploitabilityScore"),
              col("CVE_Item.impact.baseMetricV2.exploitabilityScore")
            ).as("ExploitabilityScore"),
            coalesce(
              col("CVE_Item.impact.baseMetricV3.impactScore"),
              col("CVE_Item.impact.baseMetricV2.impactScore")
            ).as("ImpactScore"),
            col("CVE_Item.publishedDate").as("PublishedDate"),
            col("CVE_Item.lastModifiedDate").as("LastModifiedDate")
          )
          .filter(col("ID").isNotNull)

        // Merge data
        mergedData = if (mergedData.isEmpty) cveDF else mergedData.union(cveDF)
      }

      // Check if mergedData is empty
      if (mergedData.isEmpty) {
        println("No data available in the merged dataset. Exiting.")
      } else {
        // Display merged data
        println("Merged Dataset:")
        mergedData.show(20, truncate = false)

        // Write merged data to a JSON file
        val outputPath = s"$inputDir/merged_cve_data.json"
        mergedData.write.mode("overwrite").json(outputPath)
        println(s"Merged data successfully written to $outputPath")
      }
    } catch {
      case e: Exception =>
        println(s"Error while processing CVE data: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
