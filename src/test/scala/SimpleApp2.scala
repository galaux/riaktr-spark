import SimpleApp.CDR
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._

object SimpleApp2 {

  val spark = SparkSession.builder.appName("Simple Application 2").getOrCreate()
  import spark.implicits._

  case class Accumulator(
                          calleeCount: Map[String, Long],
                          droppedCallsCount: Long,
                          totalCallDuration: Double,
                          internationalCallDuration: Double,
                          onNetCallDuration: Double,
                          onNetCallCount: Long,
                          lessThan10minCallCount: Long
                        )
  case class OutAccumulator(
                             distinctCalleeCount: Int,
                             top3Callees: Seq[String],
                             droppedCallsCount: Long,
                             totalCallDuration: Double,
                             internationalCallDuration: Double,
                             onNetCallAverage: Double,
                             lessThan10minCallCount: Long
                           )

  def allInOne(cdrDS: Dataset[CDR]): Dataset[(String, OutAccumulator)] = {

    val customAggregator = new Aggregator[CDR, Accumulator, OutAccumulator] {

      override def zero: Accumulator = Accumulator(Map.empty, 0L, 0.0, 0.0, 0.0, 0L, 0L)

      def reduce(acc: Accumulator, cdr: CDR): Accumulator = {
        val calleesAcc = acc.calleeCount
        val oldValue = calleesAcc.getOrElse(cdr.callee_id, 0L)
        val newCalleesAcc = calleesAcc.updated(cdr.callee_id, oldValue + 1)

        val internationalCallDuration =
          if (cdr.`type` == "international")
            acc.internationalCallDuration + cdr.duration
          else
            acc.internationalCallDuration

        val onNetCallDuration =
          if (cdr.`type` == "on-net")
            acc.onNetCallDuration + cdr.duration
          else
            acc.onNetCallDuration

        val onNetCallCount =
          if (cdr.`type` == "on-net")
            acc.onNetCallCount + 1
          else
            acc.onNetCallCount

        val lessThan10minCallCount =
          if (cdr.duration <= 10.0)
            acc.lessThan10minCallCount + 1
          else
            acc.lessThan10minCallCount

        Accumulator(
          newCalleesAcc,
          acc.droppedCallsCount + cdr.dropped,
          acc.totalCallDuration + cdr.duration,
          internationalCallDuration,
          onNetCallDuration,
          onNetCallCount,
          lessThan10minCallCount)
      }

      def merge(acc1: Accumulator, acc2: Accumulator): Accumulator = {
        val calleesAcc1 = acc1.calleeCount
        val calleesAcc2 = acc2.calleeCount
        val allKeys = calleesAcc1.keys ++ calleesAcc2.keys
        val newCalleesAcc = allKeys.map { key => (key, calleesAcc1.getOrElse(key, 0L) + calleesAcc2.getOrElse(key, 0L)) }.toMap
        Accumulator(
          newCalleesAcc,
          acc1.droppedCallsCount + acc2.droppedCallsCount,
          acc1.totalCallDuration + acc2.totalCallDuration,
          acc1.internationalCallDuration + acc2.internationalCallDuration,
          acc1.onNetCallDuration + acc2.onNetCallDuration,
          acc1.onNetCallCount + acc2.onNetCallCount,
          acc1.lessThan10minCallCount + acc2.lessThan10minCallCount)
      }

      def finish(reduction: Accumulator): OutAccumulator = {
        val distinctCalleeCount = reduction.calleeCount.size
        val top3Callees = reduction.calleeCount.toSeq.sortBy(_._2).map(_._1).reverse.take(3)
        OutAccumulator(
          distinctCalleeCount,
          top3Callees,
          reduction.droppedCallsCount,
          reduction.totalCallDuration,
          reduction.internationalCallDuration,
          reduction.onNetCallDuration / reduction.onNetCallCount,
          reduction.lessThan10minCallCount)
      }

      override def bufferEncoder: Encoder[Accumulator] = implicitly[Encoder[Accumulator]]

      override def outputEncoder: Encoder[OutAccumulator] = implicitly[Encoder[OutAccumulator]]
    }.toColumn

    cdrDS
      .groupByKey(_.caller_id)
      .agg(customAggregator)
  }
}
