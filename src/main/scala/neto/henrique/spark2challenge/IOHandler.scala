package neto.henrique.spark2challenge

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession};


/**
 *
 * Defines all of the programs IO operations Spark
 *
 * @param sparkSession The spark session
 */
class IOHandler(sparkSession: SparkSession) {

  def parseUserReviews(filename: String): DataFrame = parseCSV(filename, Schemas.rawReviewSchema)

  def parseApps(filename: String): DataFrame = parseCSV(filename, Schemas.rawAppsSchema)

  def parseCSV(filename: String, schema: StructType): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("dateFormat", "MMMM d, yyyy")
      // there is 1 malformed entry in the store dataset
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(filename)
  }

  def writeSingleCsv(df: DataFrame, filename: String, delimiter: String = "§"): Unit = {
    df.coalesce(1) // I don't like this coalesce but i need it to force a single file output ¯\_(ツ)_/¯
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", delimiter)
      .option("encoding", "UTF-8")
      .csv(filename)
  }

  def compressIntoSingleParquet(df: DataFrame, filename: String): Unit = {
    df.coalesce(1) // I don't like this coalesce but i need it to force a single file output ¯\_(ツ)_/¯
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("compression", "gzip")
      .parquet(filename)
  }

}