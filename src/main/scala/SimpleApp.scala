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

  def main(args: Array[String]) {
    // TODO load the cellDS and apply a distinct on cell_id
    val cdrDS = spark.read
      .option("header", "true")
      .schema(Encoders.product[CDR].schema)
      .csv("/enc/home/miguel/documents/it/spark/riaktr/src/test/resources/cdrs.csv") // TODO take path from args
      .as[CDR]

    val cellDS = spark.read
      .option("header", "true")
      .schema(Encoders.product[Cell].schema)
      .csv("/enc/home/miguel/documents/it/spark/riaktr/src/test/resources/cells.csv") // TODO take path from args
      .as[Cell]

    // TODO write output to file instead of printing it to stdout
    val resultStr =
      s"""Most used cell
         |==============
         |
         |${mostUsedCellByDurationPerCaller(cdrDS)}
         |
         |
         |MostUsedCellByUseCountPerCaller
         |===============================
         |
         |TODO
         |
         |
         |Number of distinct callees
         |==========================
         |
         |distinctCalleeCountPerCaller
         |
         |
         |Number of dropped calls
         |=======================
         |
         |droppedCallCountPerCaller
         |
         |
         |Total duration of the calls
         |===========================
         |
         |totalCallDurationPerCaller
         |
         |
         |Total duration of the international calls
         |=========================================
         |
         |internationalCallDurationPerCaller
         |
         |
         |Average duration of the on-net calls
         |====================================
         |
         |onNetCallAverageDurationPerCaller
         |
         |
         |Number of calls that lasted less than 10 minutes (included)
         |===========================================================
         |
         |lessThan10minCallCountPerCaller
         |
         |
         |Top-3 callee ids
         |================
         |
         |top3CalleeIdsPerCaller
       """.stripMargin
    println(resultStr)

    spark.stop()
  }

  object durationOrdering extends Ordering[(String, CellStats)] {
    def compare(a:(String, CellStats), b: (String, CellStats)) = a._2._2 compare b._2._2
  }

  object useCountOrdering extends Ordering[(String, CellStats)] {
    def compare(a:(String, CellStats), b: (String, CellStats)) = a._2._1 compare b._2._1
  }

  type CellStats = (Long, Double)

  private class CellStatsAggregator(ordering: Ordering[(String, CellStats)])
    extends Aggregator[CDR, Map[String, CellStats], (String, CellStats)] {

    override def zero: Map[String, CellStats] = Map.empty

    override def reduce(cellStats: Map[String, CellStats], cdr: CDR): Map[String, CellStats] = {
      val (prevUseCount: Long, prevDuration: Double) = cellStats.getOrElse(cdr.cell_id, (0L, 0.0))
      cellStats.updated(cdr.cell_id, (prevUseCount + 1, prevDuration + cdr.duration))
    }

    override def merge(cellUseCountA: Map[String, CellStats], cellUseCountB: Map[String, CellStats]): Map[String, CellStats] = {
      val allKeys = cellUseCountA.keys ++ cellUseCountB.keys
      allKeys.map { key =>
        val (prevUseCountA: Long, prevDurationA: Double) = cellUseCountA.getOrElse(key, (0L, 0.0))
        val (prevUseCountB: Long, prevDurationB: Double) = cellUseCountB.getOrElse(key, (0L, 0.0))
        val newStats = (prevUseCountA + prevUseCountB, prevDurationA + prevDurationB)
        (key, newStats)
      }.toMap
    }

    override def finish(reduction: Map[String, CellStats]): (String, CellStats) =
      reduction
        .toSeq
        .sorted(ordering)
        .reverse
        .head

    override def bufferEncoder: Encoder[Map[String, CellStats]] = implicitly[Encoder[Map[String, CellStats]]]

    override def outputEncoder: Encoder[(String, CellStats)] = implicitly[Encoder[(String, CellStats)]]
  }

  private def mostUsedCellPerCaller(ordering: Ordering[(String, CellStats)])(cdrDS: Dataset[CDR]): Map[String, (String, CellStats)] =
    cdrDS
      .groupByKey(_.caller_id)
      .agg(new CellStatsAggregator(ordering).toColumn)
      .collect().toMap

  val mostUsedCellByUseCountPerCaller = mostUsedCellPerCaller(useCountOrdering) _
  val mostUsedCellByDurationPerCaller = mostUsedCellPerCaller(durationOrdering) _

  def mostUsedCellCoordinates(cdrDS: Dataset[CDR], cellDS: Dataset[Cell]): Map[String, (String, Double, Double)] = {
    val mostUsedCellDS = mostUsedCellByDurationPerCaller(cdrDS)
      .mapValues(_._1)
      .toList
      .toDF("caller_id", "cell_id")
    mostUsedCellDS
      .join(cellDS, mostUsedCellDS("cell_id") === cellDS("cell_id"))
      .collect()
      .map { case Row(caller_id: String, cell_id: String, _: String, longitude: Double, latitude: Double) =>
        (caller_id, (cell_id, longitude, latitude))
      }.toMap
  }

  def expandColumns(cdrDS: Dataset[CDR]): Dataset[Row] =
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

  def distinctCalleeCountPerCaller(cdrByCaller: RelationalGroupedDataset): Map[String, Long] =
    cdrByCaller
      .agg(countDistinct("callee_id") as "callee_count")
      .collect()
      .map { case Row(caller_id: String, callee_count: Long) => (caller_id, callee_count) }
      .toMap

  def droppedCallCountPerCaller(cdrByCaller: RelationalGroupedDataset): Map[String, Long] =
    cdrByCaller
      .agg(sum("dropped") as "dropped_count")
      .collect()
      .map { case Row(caller_id: String, dropped_count: Long) => (caller_id, dropped_count) }
      .toMap

  def totalCallDurationPerCaller(cdrByCaller: RelationalGroupedDataset): Map[String, Double] =
    cdrByCaller
      .agg(sum("duration") as "total_duration")
      .collect()
      .map { case Row(caller_id: String, total_duration: Double) => (caller_id, total_duration) }
      .toMap

  def internationalCallDurationPerCaller(cdrByCaller: RelationalGroupedDataset): Map[String, Double] =
    cdrByCaller
      .agg(sum("international_duration") as "international_duration")
      .collect()
      .map { case Row(caller_id: String, total_duration: Double) => (caller_id, total_duration) }
      .toMap

  def onNetCallAverageDurationPerCaller(cdrByCaller: RelationalGroupedDataset): Map[String, Double] =
    cdrByCaller
      .agg(avg("on_net_duration") as "avg_duration")
      .collect()
      .map { case Row(caller_id: String, avg_duration: Double) => (caller_id, avg_duration) }
      .toMap

  def lessThan10minCallCountPerCaller(cdrByCaller: RelationalGroupedDataset): Map[String, Long] =
    cdrByCaller
      .agg(sum("lasts_less_than_10_min") as "less_than_10_min_duration")
      .collect()
      .map { e => (e.getAs[String](0), e.getAs[Long](1))}
      .toMap

  def top3CalleeIdsPerCaller(cdrDS: Dataset[CDR]): Map[String, Seq[String]] = {

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
      .collect().toMap
  }

}
