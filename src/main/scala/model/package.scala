import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

package object model {
  val Log = Logger.getLogger(this.getClass)
  val dataFile = "/home/knoldus/Assignment-Spark-3/src/main/resources/D1.csv"
  val TEN = 10

  def readFromCsv(spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(dataFile)
  }
}
