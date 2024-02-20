package neto.henrique.spark2challenge

import org.apache.spark.sql.DataFrame

trait ChallengeSuite extends SparkSuite {

  def appStoreTest(name: String)(f: DataFrame => Unit): Unit = {
    sparkTest(name) { spark =>
      f(new GooglePlayStoreCsvParser(spark)
        .parseApps("./play-store-datasets/googleplaystore.csv"))
    }
  }

  def userReviewTest(name: String)(f: DataFrame => Unit): Unit = {
    sparkTest(name) { spark =>
      f(new GooglePlayStoreCsvParser(spark)
        .parseUserReviews("./play-store-datasets/googleplaystore_user_reviews.csv"))
    }
  }

  def challengeTest(name: String)(f: (DataFrame, DataFrame) => Unit): Unit = {
    sparkTest(name) { spark =>
      val parser = new GooglePlayStoreCsvParser(spark)
      f(
        parser.parseApps("./play-store-datasets/googleplaystore.csv"),
        parser.parseUserReviews("./play-store-datasets/googleplaystore_user_reviews.csv")
      )
    }
  }


}
