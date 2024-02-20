package neto.henrique.spark2challenge

import org.apache.spark.sql.types._
import org.scalatest.matchers.must.Matchers

class SchemaTests extends ChallengeSuite with Matchers {

  def assertEmptySymDiff(schema1: StructType, schema2: StructType): Unit = {
    assert(schema1.diff(schema2).isEmpty)
    assert(schema2.diff(schema1).isEmpty)
  }

  private def part3schema: StructType = {
    new StructType()
      .add("App", StringType)
      .add("Categories", ArrayType(StringType, containsNull = false), nullable = false)
      .add("Rating", DoubleType)
      .add("Reviews", LongType, nullable = false)
      .add("Size", DoubleType)
      .add("Installs", StringType)
      .add("Type", StringType)
      .add("Price", DoubleType)
      .add("Content_Rating", StringType)
      .add("Genres", ArrayType(StringType, containsNull = false), nullable = false)
      .add("Last_Updated", DateType)
      .add("Current_Version", StringType)
      .add("Minimum_Android_Version", StringType)
  }


  userReviewTest("Test Output Schema :: Part 1") { df =>
    val correctSchema: StructType = new StructType()
      .add("App", StringType)
      .add("Average_Sentiment_Polarity", DoubleType, nullable = false)

    val outputDf = Challenge.part1(df)

    assertEmptySymDiff(outputDf.schema, correctSchema)
  }

  appStoreTest("Test Output Schema :: Part 2") { df =>
    val correctSchema: StructType = new StructType()
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

    val outputDf = Challenge.part2(df)

    assertEmptySymDiff(outputDf.schema, correctSchema)
  }

  appStoreTest("Test Output Schema :: Part 3") { df =>
    val correctSchema: StructType = part3schema

    val outputDf = Challenge.part3(df)

    assertEmptySymDiff(outputDf.schema, correctSchema)
  }

  challengeTest("Test Output Schema :: Part 4") { (appDF, reviewDF) =>
    val correctSchema: StructType = part3schema
      .add("Average_Sentiment_Polarity", DoubleType)

    val df1 = Challenge.part1(reviewDF)
    val df3 = Challenge.part3(appDF)
    val outputDf = Challenge.part4(df1, df3)

    assertEmptySymDiff(outputDf.schema, correctSchema)
  }

  challengeTest("Test Output Schema :: Part 5") { (appDF, reviewDF) =>
    val correctSchema: StructType = new StructType()
      .add("Genre", StringType, nullable = false)
      .add("Count", LongType, nullable = false)
      .add("Average_Rating", DoubleType)
      .add("Average_Sentiment_Polarity", DoubleType)


    val df1 = Challenge.part1(reviewDF)
    val df3 = Challenge.part3(appDF)
    val df4 = Challenge.part4(df1, df3)
    val outputDf = Challenge.part5(df4)

    assertEmptySymDiff(outputDf.schema, correctSchema)
  }

}