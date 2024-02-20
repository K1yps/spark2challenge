package neto.henrique.spark2challenge

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

trait SparkSuite extends AnyFunSuite with BeforeAndAfterAll {

  def sparkTest(name: String)(f: SparkSession => Unit): Unit = {
    this.test(name) {
      val spark = SparkSession
        .builder()
        .appName(name)
        .master("local")
        .config("spark.default.parallelism", "1")
        .getOrCreate()

      try {
        f(spark)
      } finally {
        spark.stop()
      }
    }
  }

  override def afterAll(): Unit = {
    val warehouse = new File("spark-warehouse")
    if (warehouse.exists()) warehouse.delete()
    val derby = new File("derby.log")
    if (derby.exists()) derby.delete()
  }

}