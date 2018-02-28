import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import model._

object EntryPoint extends App {
  val sc = new SparkConf().setMaster("local")
  val spark = SparkSession
    .builder()
    .config(sc)
    .getOrCreate()
  val obj = new Operations
  Log.info("\nData Frame Created")
  obj.createDataFrame(spark).show()
  Log.info("\nMatches As Home Team")
  obj.matchesAsHomeTeam(spark).show()
  Log.info("\nHighest Win Percentage")
  obj.highestWinPercentage(spark).show(TEN)
  Log.info("\nData Set Created")
  getDataSet(spark).show()
  Log.info("\nTotal Number Of Matches Played")
  obj.numberOfMatches(spark)
  Log.info("\nTop Ten Teams With Highest Wins")
  obj.highestWin(spark).show(TEN)
}
