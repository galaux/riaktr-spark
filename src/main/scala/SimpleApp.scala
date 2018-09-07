/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source



object SimpleApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

//    val logFile = "/enc/home/miguel/documents/it/spark/spark-2.3.1-bin-hadoop2.7/README.md"
//    val logData = spark.read.textFile(logFile).cache()
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    println(s"Lines with a: $numAs, Lines with b: $numBs")

    val res = doThis(spark)
    println(s"→ res: $res")

    spark.stop()
  }

  def doThis(spark: SparkSession): RDD[Int] = {
//    val cells = Source.fromURL(getClass.getResource("/cells.csv"))

    spark.sparkContext.parallelize(Seq(3, 2, 1))
  }

}
