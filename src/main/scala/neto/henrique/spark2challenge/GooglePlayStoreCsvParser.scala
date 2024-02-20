package neto.henrique.spark2challenge

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession};


/**
 *
 * Defines and parses the data schemes of the GooglePlayStore's csv
 *
 * Ensures lossless parsing
 * Maintains non standard column types as strings
 *
 * @param sparkSession The spark session
 */
class GooglePlayStoreCsvParser(sparkSession: SparkSession) {
  def rawReviewSchema: StructType = new StructType()
    .add("App", StringType)
    .add("Translated_Review", StringType)
    .add("Sentiment", StringType)
    .add("Sentiment_Polarity", DoubleType)
    .add("Sentiment_Subjectivity", DoubleType)
  def rawAppsSchema: StructType = new StructType()
    .add("App", StringType)
    .add("Category", StringType)
    .add("Rating", DoubleType)
    .add("Reviews", LongType)
    .add("Size", StringType)
    .add("Installs", StringType)
    .add("Type", StringType)
    .add("Price", StringType)
    .add("Content Rating", StringType)
    .add("Genres", StringType)
    .add("Last Updated", DateType)
    .add("Current Ver", StringType)
    .add("Android Ver", StringType)

  def parseUserReviews(filename: String): DataFrame = parseCSV(filename, rawReviewSchema)

  def parseApps(filename: String): DataFrame = parseCSV(filename, rawAppsSchema)

  private def parseCSV(filename: String, schema: StructType): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("dateFormat", "MMMM d, yyyy")
      // 2 entries have a random column missing which is not specified as empty making them malformed
      // This causes an arbitrary shift in the values of the column which will lead to data inconsistency if not handled
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(filename)
  }

}