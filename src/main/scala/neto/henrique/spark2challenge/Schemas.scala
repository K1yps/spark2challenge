package neto.henrique.spark2challenge

import org.apache.spark.sql.types._

object Schemas {

  /**
   * Defines the parsed schema for the User Reviews
   *
   * @return The schema as a StructType
   */
  def rawReviewSchema: StructType = new StructType() // Uses "def" because the type is not immutable
    .add("App", StringType)
    .add("Translated_Review", StringType)
    .add("Sentiment", StringType)
    .add("Sentiment_Polarity", DoubleType)
    .add("Sentiment_Subjectivity", DoubleType)

  /**
   * Defines the parsed schema for the Play Store app list
   *
   * @return The schema as a StructType
   */
  def rawAppsSchema: StructType = new StructType() // Uses "def" because the type is not immutable
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
    .add("Android Ver", StringType, nullable = false)

}
