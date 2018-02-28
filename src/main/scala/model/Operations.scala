package model

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

class Operations {
  def createDataFrame(spark: SparkSession): DataFrame = readFromCsv(spark)

  def matchesAsHomeTeam(spark: SparkSession): DataFrame = {
    val frames = readFromCsv(spark).select("HomeTeam").withColumn("count", lit(1))
    frames.createOrReplaceTempView("data")
    spark.sql("SELECT HomeTeam, SUM(count) from data group by HomeTeam order by SUM(count) DESC")
  }

  def highestWinPercentage(spark: SparkSession): DataFrame = {
    val rawData = readFromCsv(spark).select("HomeTeam", "AwayTeam", "FTR").createOrReplaceTempView("matches")
    val awayWins = spark.sql("select AwayTeam, sum(case when FTR = 'A' then 1 else 0 end) as awayWins, count(*) as totalMatches from matches group by AwayTeam")
    val homeWins = spark.sql("select HomeTeam, sum(case when FTR = 'H' then 1 else 0 end) as homeWins, count(*) as totalMatches from matches group by HomeTeam")
    homeWins.createOrReplaceTempView("homeWinsData")
    awayWins.createOrReplaceTempView("awayWinsData")
    spark.sql("select HomeTeam ,round((homeWins + awayWins) * 100 / (homeWinsView.totalMatches + awayWinsView.totalMatches), 2) as win_percentage  " +
      "from homeWinsView join awayWinsView on homeWinsView.HomeTeam = awayWinsView.AwayTeam order by win_percentage DESC")
  }

  def highestWin(spark: SparkSession): DataFrame = {
    val rawData = getDataSet(spark)
    val homeWins = rawData.select("*").where("FTR = 'H'").withColumn("count", lit(1))
    homeWins.createOrReplaceTempView("HomeWins")
    val awayWins = rawData.select("*").where("FTR = 'A'").withColumn("count", lit(1))
    awayWins.createOrReplaceTempView("AwayWins")
    spark.sql("select Team, sum(count) as Wins from (select * from HomeWins union select * from AwayWins) " +
      "as totalWins group by Team order by Wins DESC")
  }

  def numberOfMatches(spark: SparkSession):DataFrame = {
    val rawData = getDataSet(spark)
    val homeMatches = rawData.select("homeTeam").withColumn("count", lit(1))
    val awayMatches = rawData.select("awayTeam").withColumn("count", lit(1))
    homeMatches.createOrReplaceTempView("homeData")
    awayMatches.createOrReplaceTempView("awayData")
    spark.sql("select HomeTeam ,(homeWins + awayWins) from homeWinsView join awayWinsView on homeWinsView.HomeTeam = awayWinsView.AwayTeam")
  }
}
