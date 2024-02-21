package neto.henrique.spark2challenge

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object App {

  val log: Logger = Logger.getLogger("spark2-challenge-app")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark2-challenge-app").master("local").getOrCreate()

    try {
      val io = new IOHandler(spark)

      log.trace("Parsing Datasets")

      val reviewDF = io.parseUserReviews("./play-store-datasets/googleplaystore_user_reviews.csv")
      val storeDF = io.parseApps("./play-store-datasets/googleplaystore.csv")

      log.trace("Building DataFrames")

      val df1 = Challenge.part1(reviewDF)
      val df2 = Challenge.part2(storeDF)
      val df3 = Challenge.part3(storeDF)
      val df4 = Challenge.part4(df1, df3)
      val df5 = Challenge.part5(df4)

      log.trace("Running and Writing DF2 to best_apps")
      io.writeSingleCsv(df2, "./output/best_apps")

      log.trace("Running and Writing DF4 to googleplaystore_cleaned")
      io.compressIntoSingleParquet(df4, "./output/googleplaystore_cleaned")

      log.trace("Running and Writing DF5 to googleplaystore_cleaned")
      io.compressIntoSingleParquet(df5, "./output/googleplaystore_metrics")

    } finally {
      spark.stop()
    }
  }

}