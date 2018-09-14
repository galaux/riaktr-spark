/* SimpleApp.scala */
import java.io.{File, PrintWriter}

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._


object SimpleApp {


  val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
  import spark.implicits._

  case class Cell(cell_id: String,
                  longitude: Option[Double],
                  latitude: Option[Double])
  case class CDR(caller_id: String,
                 callee_id: String,
                 cell_id: String,
                 duration: Double,
                 `type`: String,
                 dropped: Int)
  case class ExtendedCDR(caller_id: String,
                         callee_id: String,
                         cell_id: String,
                         longitude: Option[Double],
                         latitude: Option[Double],
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
                   |Schema: (<caller_id>,(Cell(<cell_id>,<cell_longitude>,<cell_latitude>),CellAccumulator(<cell_use_count>,<cell_use_duration>)))
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

  object durationOrdering extends Ordering[(Cell, CellAccumulators)] {
    def compare(a:(Cell, CellAccumulators), b: (Cell, CellAccumulators)) = a._2.totalDuration compare b._2.totalDuration
  }

  object useCountOrdering extends Ordering[(Cell, CellAccumulators)] {
    def compare(a:(Cell, CellAccumulators), b: (Cell, CellAccumulators)) = a._2.useCount compare b._2.useCount
  }

  case class CellAccumulators(useCount: Long = 0L, totalDuration: Double = 0.0)

  private class CellAggregator(ordering: Ordering[(Cell, CellAccumulators)])
    extends Aggregator[ExtendedCDR, Map[Cell, CellAccumulators], (Cell, CellAccumulators)] {

    override def zero: Map[Cell, CellAccumulators] = Map.empty

    override def reduce(accumulators: Map[Cell, CellAccumulators], cdr: ExtendedCDR): Map[Cell, CellAccumulators] = {
      val cell = Cell(cdr.cell_id, cdr.longitude, cdr.latitude)
      val prevAccumulators = accumulators.getOrElse(cell, CellAccumulators())
      val nextAccumulators = CellAccumulators(prevAccumulators.useCount + 1, prevAccumulators.totalDuration + cdr.duration)
      accumulators.updated(cell, nextAccumulators)
    }

    override def merge(accumulatorsListA: Map[Cell, CellAccumulators], accumulatorsListB: Map[Cell, CellAccumulators]): Map[Cell, CellAccumulators] = {
      val allKeys = accumulatorsListA.keys ++ accumulatorsListB.keys
      allKeys.map { cell =>
        val accumulatorsA = accumulatorsListA.getOrElse(cell, CellAccumulators())
        val accumulatorsB = accumulatorsListB.getOrElse(cell, CellAccumulators())
        val nextAccumulators = CellAccumulators(accumulatorsA.useCount + accumulatorsB.useCount, accumulatorsA.totalDuration + accumulatorsB.totalDuration)
        (cell, nextAccumulators)
      }.toMap
    }

    override def finish(reduction: Map[Cell, CellAccumulators]): (Cell, CellAccumulators) =
      reduction
        .toSeq
        .sorted(ordering)
        .reverse
        .head

    override def bufferEncoder: Encoder[Map[Cell, CellAccumulators]] = implicitly[Encoder[Map[Cell, CellAccumulators]]]

    override def outputEncoder: Encoder[(Cell, CellAccumulators)] = implicitly[Encoder[(Cell, CellAccumulators)]]
  }

  private def mostUsedCellPerCaller(ordering: Ordering[(Cell, CellAccumulators)])(cdrDS: Dataset[CDR], cellDS: Dataset[Cell]): Dataset[(String, (Cell, CellAccumulators))] =
    cdrDS.join(cellDS, Seq("cell_id"), "left_outer")
      .as[ExtendedCDR]
      .groupByKey(_.caller_id)
      .agg(new CellAggregator(ordering).toColumn)

  /**
    * Return metrics over the most used cell per caller.
    *
    * This version compares cells by the number of time they were used.
    *
    * @param cdrDS the CDR dataset
    * @param cellDS the cell dataset
    * @return a Dataset of caller id to a tuple of cell (id and coordinates) and accumulated values
    *         (use count and duration count)
    */
  val mostUsedCellByUseCountPerCaller = mostUsedCellPerCaller(useCountOrdering) _

  /**
    * Compute metrics over the most used cell per caller.
    *
    * This version compares cells by their total call duration.
    *
    * @param cdrDS the CDR dataset
    * @param cellDS the cell dataset
    * @return a Dataset of caller id to a tuple of cell (id and coordinates) and accumulated values
    *         (use count and duration count)
    */
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

  /**
    * Compute common trivial metrics per caller.
    *
    * @param cdrDS the CDR dataset
    * @return a DataFrame of: caller_id, distinct_callee_count, dropped_call_count,
    *         total_call_duration, international_call_duration, avg_on_net_call_duration,
    *         less_than_10_min_call_count
    */
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


  /**
    * Compute the top-3 callees per caller.
    *
    * @param cdrDS the CDR dataset
    * @return a Dataset of caller_id to a list of callee_ids
    */
  def top3CalleeIdsPerCaller(cdrDS: Dataset[CDR]): Dataset[(String, Seq[String])] = {

    val top3CalleeIdsAgg = new Aggregator[CDR, Map[String, Long], Seq[String]] {

      override def zero: Map[String, Long] = Map.empty

      override def reduce(accumulatorsList: Map[String, Long], cdr: CDR): Map[String, Long] = {
        val prevAccumulators = accumulatorsList.getOrElse(cdr.callee_id, 0L)
        accumulatorsList.updated(cdr.callee_id, prevAccumulators + 1)
      }

      override def merge(accumulatorListA: Map[String, Long], accumulatorListB: Map[String, Long]): Map[String, Long] = {
        val allKeys = accumulatorListA.keys ++ accumulatorListB.keys
        allKeys.map { key => (key, accumulatorListA.getOrElse(key, 0L) + accumulatorListB.getOrElse(key, 0L)) }.toMap
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
