import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import model._

object EntryPoint extends App {
  val sc = new SparkConf().setMaster("local")
  val spark = SparkSession
    .builder()
    .config(sc)
    .getOrCreate()
  val obj = new Operations
  obj.createDataFrame(spark)
//  obj.matchesAsHomeTeam(spark)
  obj.highestWin(spark)
}
