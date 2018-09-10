/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}




object SimpleApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    spark.sqlContext.udf.register("strToBoolean", (s: String) => s.toBoolean)

//    val logFile = "/enc/home/miguel/documents/it/spark/spark-2.3.1-bin-hadoop2.7/README.md"
//    val logData = spark.read.textFile(logFile).cache()
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    println(s"Lines with a: $numAs, Lines with b: $numBs")

    // Load the (typed!) DataFrames(?) here and pass them to the functions

//    val df = spark.read
//      .option("header", "true")
//      .csv("/enc/home/miguel/documents/it/spark/riaktr/src/test/resources/cells.csv")
//    val resDf = mostUsedCells(df)
//    println(s"→ res: $resDf")

    spark.stop()
  }

  case class Cell(cell_id: String, longitude: String, latitude: String)
  case class CDR(caller_id: String,
                 callee_id: String,
                 cell_id: String,
                 duration: Double,
                 `type`: String,
                 dropped: Int
                )

  def mostUsedCells(df: Dataset[CDR]): DataFrame = {
    df.select("cell_id")
  }

  def distinctCalleeCount(cdrDS: Dataset[CDR]): Long =
    cdrDS.select("callee_id").distinct().count()

  def droppedCallCount(cdrDS: Dataset[CDR]): Long =
    cdrDS.filter(_.dropped > 0).count()

}
