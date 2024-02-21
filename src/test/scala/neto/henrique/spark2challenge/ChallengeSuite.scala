package neto.henrique.spark2challenge

import org.apache.spark.sql.DataFrame

trait ChallengeSuite extends SparkSuite {

  //Ideally I would be generating Mock Data instead of using the real dataset

  def appStoreTest(name: String)(f: DataFrame => Unit): Unit = {
    sparkTest(name) { spark =>
      f(new IOHandler(spark).parseApps("./play-store-datasets/googleplaystore.csv"))
    }
  }

  def userReviewTest(name: String)(f: DataFrame => Unit): Unit = {
    sparkTest(name) { spark =>
      f(new IOHandler(spark).parseUserReviews("./play-store-datasets/googleplaystore_user_reviews.csv"))
    }
  }

  def challengeTest(name: String)(f: (DataFrame, DataFrame) => Unit): Unit = {
    sparkTest(name) { spark =>
      val io = new IOHandler(spark)
      f(
        io.parseApps("./play-store-datasets/googleplaystore.csv"),
        io.parseUserReviews("./play-store-datasets/googleplaystore_user_reviews.csv")
      )
    }
  }

}
