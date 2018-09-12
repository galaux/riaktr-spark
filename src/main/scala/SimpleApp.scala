/* SimpleApp.scala */
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._


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

  object durationOrdering extends Ordering[(String, (Long, Double))] {
    def compare(a:(String, (Long, Double)), b: (String, (Long, Double))) = a._2._2 compare b._2._2
  }

  object useCountOrdering extends Ordering[(String, (Long, Double))] {
    def compare(a:(String, (Long, Double)), b: (String, (Long, Double))) = a._2._1 compare b._2._1
  }

  def main(args: Array[String]) {
    // FIXME wrong file opened. This is a WIP
    val cdrDS = spark.read
      .option("header", "true")
      .csv("/enc/home/miguel/documents/it/spark/riaktr/src/test/resources/cells.csv")
      .as[CDR]
    println(
      s"""- Most used cell:\t\t${mostUsedCellByDurationPerCaller(cdrDS)}
         |- Dictinct callee count:\t
       """.stripMargin)

    spark.stop()
  }

  class CellStatsAggregator(ordering: Ordering[(String, (Long, Double))])
    extends Aggregator[CDR, Map[String, (Long, Double)], (String, (Long, Double))] {

    override def zero: Map[String, (Long, Double)] = Map.empty

    override def reduce(cellStats: Map[String, (Long, Double)], cdr: CDR): Map[String, (Long, Double)] = {
      val (prevUseCount: Long, prevDuration: Double) = cellStats.getOrElse(cdr.cell_id, (0L, 0.0))
      cellStats.updated(cdr.cell_id, (prevUseCount + 1, prevDuration + cdr.duration))
    }

    override def merge(cellUseCountA: Map[String, (Long, Double)], cellUseCountB: Map[String, (Long, Double)]): Map[String, (Long, Double)] = {
      val allKeys = cellUseCountA.keys ++ cellUseCountB.keys
      allKeys.map { key =>
        val (prevUseCountA: Long, prevDurationA: Double) = cellUseCountA.getOrElse(key, (0L, 0.0))
        val (prevUseCountB: Long, prevDurationB: Double) = cellUseCountB.getOrElse(key, (0L, 0.0))
        val newStats = (prevUseCountA + prevUseCountB, prevDurationA + prevDurationB)
        (key, newStats)
      }.toMap
    }

    override def finish(reduction: Map[String, (Long, Double)]): (String, (Long, Double)) =
      reduction
        .toSeq
        .sorted(ordering)
        .reverse
        .head

    override def bufferEncoder: Encoder[Map[String, (Long, Double)]] = implicitly[Encoder[Map[String, (Long, Double)]]]

    override def outputEncoder: Encoder[(String, (Long, Double))] = implicitly[Encoder[(String, (Long, Double))]]
  }

  private def mostUsedCellPerCaller(ordering: Ordering[(String, (Long, Double))])(cdrDS: Dataset[CDR]): Map[String, (String, (Long, Double))] =
    cdrDS
      .groupByKey(_.caller_id)
      .agg(new CellStatsAggregator(ordering).toColumn)
      .collect().toMap

  val mostUsedCellByUseCountPerCaller = mostUsedCellPerCaller(useCountOrdering) _
  val mostUsedCellByDurationPerCaller = mostUsedCellPerCaller(durationOrdering) _

  def distinctCalleeCountPerCaller(cdrDS: Dataset[CDR]): Map[String, Long] =
    cdrDS.groupBy("caller_id")
      .agg(countDistinct("callee_id") as "callee_count")
      .collect()
      .map { case Row(caller_id: String, callee_count: Long) => (caller_id, callee_count) }
      .toMap

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

//  TODO the latitude and longitude of the most used cell

  def lessThan10minCallCountPerCaller(cdrDS: Dataset[CDR]): Map[String, Long] =
    cdrDS.filter(_.duration <= 10.0)
      .groupBy("caller_id")
      .count()
      .collect()
      .map { e => (e.getAs[String](0), e.getAs[Long](1))}
      .toMap

  def top3CalleeIdsPerCaller(cdrDS: Dataset[CDR]): Map[String, Seq[String]] = {

    val top3CalleeIdsAgg = new Aggregator[CDR, Map[String, Int], Seq[String]] {

      override def zero: Map[String, Int] = Map.empty

      override def reduce(calleeCallCount: Map[String, Int], cdr: CDR): Map[String, Int] = {
        val oldValue = calleeCallCount.getOrElse(cdr.callee_id, 0)
        calleeCallCount.updated(cdr.callee_id, oldValue + 1)
      }

      override def merge(calleeCallCountA: Map[String, Int], calleeCallCountB: Map[String, Int]): Map[String, Int] = {
        val allKeys = calleeCallCountA.keys ++ calleeCallCountB.keys
        allKeys.map { key => (key, calleeCallCountA.getOrElse(key, 0) + calleeCallCountB.getOrElse(key, 0)) }.toMap
      }

      override def finish(reduction: Map[String, Int]): Seq[String] =
        reduction.toSeq
          .sortWith { case ((_, countA), (_, countB)) => countA > countB }
          .take(3)
          .map { case (callee_id, _) => callee_id }

      override def bufferEncoder: Encoder[Map[String, Int]] = implicitly[Encoder[Map[String, Int]]]

      override def outputEncoder: Encoder[Seq[String]] = implicitly[Encoder[Seq[String]]]
    }.toColumn

    cdrDS
      .groupByKey(_.caller_id)
      .agg(top3CalleeIdsAgg)
      .collect().toMap
  }

}
