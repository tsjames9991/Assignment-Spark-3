import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

package object model {
  val Log = Logger.getLogger(this.getClass)
  val dataFile = "/home/knoldus/Assignment-Spark-3/src/main/resources/D1.csv"
  val TEN = 10

  def getDataSet(spark: SparkSession): Dataset[FootballData] = {
    val dFrame = readFromCsv(spark)
    import spark.implicits._
    dFrame.map {
      row => {
        FootballData(
          row.getAs[String]("homeTeam"),
            row.getAs[String]("awayTeam"),
        row.getAs[Int]("homeGoals"),
        row.getAs[Int]("awayGoals"),
        row.getAs[String]("result")
        )
      }
    }
  }

  def readFromCsv(spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").option("inferSchema", "true").csv(dataFile)
  }

  case class FootballData(
                           homeTeam: String,
                           awayTeam: String,
                           homeGoals: Int,
                           awayGoals: Int,
                           result: String
                         )
}
