/* SimpleApp.scala */
import java.io.{File, PrintWriter}

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
                 dropped: Int)
  case class ExtendedCDR(caller_id: String,
                         callee_id: String,
                         cell_id: String,
                         longitude: Double,
                         latitude: Double,
                         duration: Double,
                         `type`: String,
                         dropped: Int)

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }


  def main(args: Array[String]) {

    if (args.length != 3) {
      System.err.println(
        """Not enough parameters.
          |Please provide:
          |- cdrs.csv file path
          |- cells.csv file path
          |- output (txt) file path
        """.stripMargin)
      System.out.println(args.toList)
      System.exit(1)
    }

    val cdrsPath = args(0)
    val cellsPath = args(1)
    val outPath = args(2)

    System.out.println(
      s"""Using:
        |- cdrs file:\t$cdrsPath
        |- cells file:\t$cellsPath
        |- output file:\t$outPath
      """.stripMargin)

    val cdrDS = spark.read
      .option("header", "true")
      .schema(Encoders.product[CDR].schema)
      .csv(cdrsPath)
      .as[CDR]

    val cellDS = spark.read
      .option("header", "true")
      .schema(Encoders.product[Cell].schema)
      .csv(cellsPath)
      .as[Cell]

    printToFile(new File(outPath)) { p =>
      p.println("""|Most used cell by duration per caller
                   |=====================================
                   |
                   |Schema: (<caller_id>,(CellId(<cell_id>,<cell_longitude>,<cell_latitude>),CellStats(<cell_use_count>,<cell_use_duration>)))
                   |
        """.stripMargin)
      mostUsedCellByDurationPerCaller(cdrDS, cellDS).collect().map(p.println)

      p.println("""
                  |
                  |Top-3 callee ids
                  |================
                  |
                  |Schema: (<caller_id>,<list_of_callee_ids>)
                  |
        """.stripMargin)
      top3CalleeIdsPerCaller(cdrDS).collect().map(p.println)

      p.println("""
                  |
                  |Common metrics
                  |==============
                  |
                  |Schema: [<caller_id>,<distinct_callee_count>,<dropped_call_count>,<total_call_duration>,<international_call_duration>,<avg_on_net_call_duration>,<less_than_10_min_call_count>]
                  |
        """.stripMargin)
      commonMetrics(cdrDS).collect().map(p.println)
    }

    spark.stop()
  }

  object durationOrdering extends Ordering[(CellId, CellStats)] {
    def compare(a:(CellId, CellStats), b: (CellId, CellStats)) = a._2.totalDuration compare b._2.totalDuration
  }

  object useCountOrdering extends Ordering[(CellId, CellStats)] {
    def compare(a:(CellId, CellStats), b: (CellId, CellStats)) = a._2.useCount compare b._2.useCount
  }

  case class CellStats(useCount: Long = 0L, totalDuration: Double = 0.0)

  case class CellId(id: String, longitude: Double, latitude: Double)

  private class CellStatsAggregator(ordering: Ordering[(CellId, CellStats)])
    extends Aggregator[ExtendedCDR, Map[CellId, CellStats], (CellId, CellStats)] {

    override def zero: Map[CellId, CellStats] = Map.empty

    override def reduce(cellStatsList: Map[CellId, CellStats], cdr: ExtendedCDR): Map[CellId, CellStats] = {
      val cellId = CellId(cdr.cell_id, cdr.longitude, cdr.latitude)
      val prevCellStats = cellStatsList.getOrElse(cellId, CellStats())
      val newCellStats = CellStats(prevCellStats.useCount + 1, prevCellStats.totalDuration + cdr.duration)
      cellStatsList.updated(cellId, newCellStats)
    }

    override def merge(cellUseCountA: Map[CellId, CellStats], cellUseCountB: Map[CellId, CellStats]): Map[CellId, CellStats] = {
      val allKeys = cellUseCountA.keys ++ cellUseCountB.keys
      allKeys.map { key =>
        val cellStatsA = cellUseCountA.getOrElse(key, CellStats())
        val cellStatsB = cellUseCountB.getOrElse(key, CellStats())
        val newStats = CellStats(cellStatsA.useCount + cellStatsB.useCount, cellStatsA.totalDuration + cellStatsB.totalDuration)
        (key, newStats)
      }.toMap
    }

    override def finish(reduction: Map[CellId, CellStats]): (CellId, CellStats) =
      reduction
        .toSeq
        .sorted(ordering)
        .reverse
        .head

    override def bufferEncoder: Encoder[Map[CellId, CellStats]] = implicitly[Encoder[Map[CellId, CellStats]]]

    override def outputEncoder: Encoder[(CellId, CellStats)] = implicitly[Encoder[(CellId, CellStats)]]
  }

  private def mostUsedCellPerCaller(ordering: Ordering[(CellId, CellStats)])(cdrDS: Dataset[CDR], cellDS: Dataset[Cell]): Dataset[(String, (CellId, CellStats))] =
    cdrDS.join(cellDS, "cell_id")
      .as[ExtendedCDR]
      .groupByKey(_.caller_id)
      .agg(new CellStatsAggregator(ordering).toColumn)

  val mostUsedCellByUseCountPerCaller = mostUsedCellPerCaller(useCountOrdering) _
  val mostUsedCellByDurationPerCaller = mostUsedCellPerCaller(durationOrdering) _

  def expandColsForCommonMetrics(cdrDS: Dataset[CDR]): Dataset[Row] =
    cdrDS
      .withColumn(
        "international_duration",
        when($"type" === "international", col("duration")))
      .withColumn(
        "on_net_duration",
        when($"type" === "on-net", col("duration")))
      .withColumn(
        "lasts_less_than_10_min",
        when($"duration" <= 10.0, 1).otherwise(0))

  def commonMetrics(cdrDS: Dataset[CDR]): DataFrame =
    expandColsForCommonMetrics(cdrDS)
      .groupBy("caller_id")
      .agg(
        countDistinct("callee_id") as "distinct_callee_count",
        sum("dropped") as "dropped_call_count",
        sum("duration") as "total_call_duration",
        sum("international_duration") as "international_call_duration",
        avg("on_net_duration") as "avg_on_net_call_duration",
        sum("lasts_less_than_10_min") as "less_than_10_min_call_count")


  def top3CalleeIdsPerCaller(cdrDS: Dataset[CDR]): Dataset[(String, Seq[String])] = {

    val top3CalleeIdsAgg = new Aggregator[CDR, Map[String, Long], Seq[String]] {

      override def zero: Map[String, Long] = Map.empty

      override def reduce(calleeCallCount: Map[String, Long], cdr: CDR): Map[String, Long] = {
        val oldValue = calleeCallCount.getOrElse(cdr.callee_id, 0L)
        calleeCallCount.updated(cdr.callee_id, oldValue + 1)
      }

      override def merge(calleeCallCountA: Map[String, Long], calleeCallCountB: Map[String, Long]): Map[String, Long] = {
        val allKeys = calleeCallCountA.keys ++ calleeCallCountB.keys
        allKeys.map { key => (key, calleeCallCountA.getOrElse(key, 0L) + calleeCallCountB.getOrElse(key, 0L)) }.toMap
      }

      override def finish(reduction: Map[String, Long]): Seq[String] =
        reduction.toSeq
          .sortWith { case ((_, countA), (_, countB)) => countA > countB }
          .take(3)
          .map { case (callee_id, _) => callee_id }

      override def bufferEncoder: Encoder[Map[String, Long]] = implicitly[Encoder[Map[String, Long]]]

      override def outputEncoder: Encoder[Seq[String]] = implicitly[Encoder[Seq[String]]]
    }.toColumn

    cdrDS
      .groupByKey(_.caller_id)
      .agg(top3CalleeIdsAgg)
  }

}
