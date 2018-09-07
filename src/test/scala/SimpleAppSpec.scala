import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DataframeGenerator, RDDComparisons, SharedSparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class SimpleAppSpec
  extends FunSpec
//    with SharedSparkContext
  with DataFrameSuiteBase
    with RDDComparisons
    with BeforeAndAfterEach
{

  var sparkSession: SparkSession = _

  override def beforeEach() {
    sparkSession = SparkSession.builder()
      .appName("udf testings")
      .master("local")
      //      .config("", "")
      .getOrCreate()
  }

  override def afterEach() {
    sparkSession.stop()
  }

  describe("haha") {

    it("osef") {

      import sqlContext.implicits._

//      val resultRDD = SimpleApp.doThis(sparkSession)
//      assertRDDEquals(expectedRDD, resultRDD)
//      //      assertRDDEqualsWithOrder(expectedRDD, resultRDD)

//      // Only comparing RDDs work ===================================================
//val expectedDF = sparkSession.sparkContext.parallelize(Seq("A3245", "A2153", "A9481")).toDF
//      val df = sparkSession.read
//        .option("header", "true")
//        .csv("/enc/home/miguel/documents/it/spark/riaktr/src/test/resources/cells.csv")
//      val resDf = SimpleApp.doThis(df)
//      assertRDDEquals(expectedDF.rdd, resDf.rdd)
////      assertDataFrameEquals(expectedDF, resDf)
//      // ============================================================================

      // Trying to compare DataFrames ===============================================
      val expectedDF = sparkSession.sparkContext
        .parallelize(Seq("A3245", "A2153", "A9481"))
        .toDF("cell_id")
      val df = sparkSession.read
        .option("header", "true")
        .csv("/enc/home/miguel/documents/it/spark/riaktr/src/test/resources/cells.csv")
      val resDf = SimpleApp.doThis(df)
      assertDataFrameEquals(expectedDF, resDf)
      // ============================================================================

//      println(":::")
//      val cells: DataFrame = sparkSession.read
//        .option("header", "true")
//        .csv("src/test/resources/cells.csv")
//      println(cells.getClass)
//      println(cells.take(100))

    }

//    it("haha") {
//      //      sparkSession.read.csv("cells.csv")
//      val cells = sparkSession.read
////        .option("header", "false")
//        .csv("src/test/resources/cells.csv")
//      println("... " + cells.toDF().getClass)
//      println(s"→ 'cells' is a ${cells.getClass}")
//      cells.take(10).map(println)
//    }

  }

}
