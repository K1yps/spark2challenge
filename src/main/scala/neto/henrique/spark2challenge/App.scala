package neto.henrique.spark2challenge

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark2-challenge-app").master("local").getOrCreate()

    try {
      val parser = new GooglePlayStoreCsvParser(spark)

      val reviewDF = parser.parseUserReviews("./play-store-datasets/googleplaystore_user_reviews.csv")
      val storeDF = parser.parseApps("./play-store-datasets/googleplaystore.csv")

      val df1 = Challenge.part1(reviewDF)
      val df2 = Challenge.part2(storeDF)
      val df3 = Challenge.part3(storeDF)
      val df4 = Challenge.part4(df1, df3)
      val df5 = Challenge.part5(df4)

      df2
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("delimiter", "§")
        .option("encoding", "UTF-8")
        .csv("./output/best_apps.csv")

      df3
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("compression", "gzip")
        .parquet("./output/googleplaystore_cleaned.parquet")


      df5
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", "gzip")
        .parquet("./output/googleplaystore_metrics.parquet")

    } finally {
      spark.stop()
    }
  }

}