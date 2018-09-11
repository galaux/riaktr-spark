/* SimpleApp.scala */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}


object SimpleApp {


  val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
  import spark.implicits._

  case class Cell(cell_id: String,
                  longitude: Double,
                  latitude: Double)
  case class CDR(caller_id: String,
                 callee_id: String,
                 cell_id: String,
                 duration: Double,
                 `type`: String,
                 dropped: Int
                )

  def main(args: Array[String]) {
    // FIXME wrong file opened. This is a WIP
    val cdrDS = spark.read
      .option("header", "true")
      .csv("/enc/home/miguel/documents/it/spark/riaktr/src/test/resources/cells.csv")
      .as[CDR]
    println(
      s"""- Most used cell:\t\t${mostUsedCell(cdrDS)}
         |- Dictinct callee count:\t
       """.stripMargin)

    spark.stop()
  }

  /**
    * Computes the most used cell by the number of calls
    * @param cdrDS the CDR dataset
    * @return the most used cell by the number of calls
    */
  def mostUsedCell(cdrDS: Dataset[CDR]): String =
    cdrDS.groupBy($"cell_id")
      .count()
      .sort($"count".desc)
      .first()
      .getAs[String]("cell_id")

  def distinctCalleeCount(cdrDS: Dataset[CDR]): Long =
    cdrDS.select("callee_id").distinct().count()

  def droppedCallCountPerCaller(cdrDS: Dataset[CDR]): Map[String, Long] =
    cdrDS.filter(_.dropped > 0)
      .groupBy("caller_id")
      .agg(count("dropped") as "dropped_count")
      .collect()
      .map { case Row(caller_id: String, dropped_count: Long) => (caller_id, dropped_count) }
      .toMap

  def totalCallDurationPerCaller(cdrDS: Dataset[CDR]): Map[String, Double] =
    cdrDS.groupBy("caller_id")
      .agg(sum("duration") as "total_duration")
      .collect()
      .map { case Row(caller_id: String, total_duration: Double) => (caller_id, total_duration) }
      .toMap

  def internationalCallDurationPerCaller(cdrDS: Dataset[CDR]): Map[String, Double] =
    totalCallDurationPerCaller(cdrDS.filter(_.`type` == "international"))

  def onNetCallAverageDurationPerCaller(cdrDS: Dataset[CDR]): Map[String, Double] =
    cdrDS.filter(_.`type` == "on-net")
      .groupBy("caller_id")
      .agg(avg("duration") as "avg_duration")
      .collect()
      .map { case Row(caller_id: String, avg_duration: Double) => (caller_id, avg_duration) }
      .toMap

  def lessThan10minCallCount(cdrDS: Dataset[CDR]): Long =
    cdrDS.filter(_.duration <= 10.0).count()

  def top3CalleeIds(cdrDS: Dataset[CDR]): Seq[String] =
    cdrDS.groupBy($"callee_id")
      .count()
      .sort($"count".desc)
      .take(3)
      .map { case Row(callee_id: String, _: Long) => callee_id }

}
