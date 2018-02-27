package model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class Operations {
  def createDataFrame(spark: SparkSession) = {
    val dFrame: DataFrame = readFromCsv(spark)
    Log.info("Data From The CSV File :\n")
    dFrame.show()
  }

  def matchesAsHomeTeam(spark: SparkSession): DataFrame = {
    val frames = readFromCsv(spark).select("HomeTeam").withColumn("count", lit(1))
    frames.createOrReplaceTempView("data")
    spark.sql("SELECT HomeTeam, SUM(count) from data group by HomeTeam order by SUM(count) DESC")
  }

//  def highestWin(spark: SparkSession) = {
//    val home = readFromCsv(spark).select("Date", "HomeTeam", "FTR").toDF("Date", "Team","FTR").where("FTR = 'H'").withColumn("count", lit(1))
//    home.createOrReplaceTempView("data1")
//    val awayFrames = readFromCsv(spark).select("Date", "AwayTeam", "FTR").toDF("Date", "Team","FTR").where("FTR = 'A'").withColumn("count", lit(1))
//    awayFrames.createOrReplaceTempView("data2")
//    val result = spark.sql("select Team, sum(count) as Wins from (select * from data1 union select * from data2) as joineddata group by Team order by Wins DESC")
//    result.show(10)
//  }

  def highestWinPercentage(spark: SparkSession) = {

  }
}
