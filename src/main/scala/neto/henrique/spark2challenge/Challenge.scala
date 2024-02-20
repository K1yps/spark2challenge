package neto.henrique.spark2challenge

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame}

object Challenge {
  def part1(reviewDF: DataFrame): DataFrame = {
    reviewDF
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
      .na
      .fill(0, Seq("Average_Sentiment_Polarity"))
  }

  def part2(storeDF: DataFrame): DataFrame = {
    def validAndGTE4(c: Column) = !c.isNaN && c.isNotNull && c >= 4.0

    storeDF
      .filter(validAndGTE4(storeDF("Rating")))
      .sort(col("Rating").desc)
  }

  def part3(storeDF: DataFrame): DataFrame = {
    //Note: I am already parsing the dates as Date objects
    storeDF
      //Split "Genres" into an array
      .withColumn("Genres", split(col("Genres"), ";"))
      //Covert Size to a double based on M/k scale (Invalid numbers will be null)
      .withColumn("Size", appSizeAsDouble(col("Size")))
      //Cleanup Price and covert to €
      .withColumn("Price", regexp_replace(col("Price"), "\\$", "").cast(DoubleType).*(0.9))
      //GroupBy App and Rename
      .groupBy("App")
      .agg(
        collect_set("Category").as("Categories"),
        max("Rating").as("Rating"),
        last("Reviews").alias("Reviews"),
        last("Size").alias("Size"),
        last("Installs").alias("Installs"),
        last("Type").alias("Type"),
        last("Price").alias("Price"),
        last("Content Rating").alias("Content_Rating"),
        array_distinct(flatten(collect_set("Genres"))).alias("Genres"),
        max("Last Updated").alias("Last_Updated"),
        max("Current Ver").alias("Current_Version"),
        max("Android Ver").alias("Minimum_Android_Version")
      )
      .na.fill(0, Seq("Reviews"))
  }

  private def appSizeAsDouble(c: Column): Column = {
    regexp_extract(c, "(\\d+\\.?\\d*)", 1)
      .cast(DoubleType)
      .*(when(c.endsWith("k"), 1e-3).otherwise(1.0))
  }

  def part4(df1: DataFrame, df3: DataFrame): DataFrame = {
    df3
      .join(df1, "App", "left")
      .select(df3.col("*"), df1.col("Average_Sentiment_Polarity"))
  }


  def part5(df4: DataFrame): DataFrame = {
    df4
      .withColumn("Genre", explode(col("Genres")))
      .groupBy("Genre")
      .agg(
        countDistinct("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )
  }

}
