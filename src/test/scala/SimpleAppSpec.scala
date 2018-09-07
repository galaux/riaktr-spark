import com.holdenkarau.spark.testing.{DataframeGenerator, RDDComparisons, SharedSparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class SimpleAppSpec
  extends FunSpec
//    with SharedSparkContext
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
      val expectedRDD = sparkSession.sparkContext.parallelize(Seq(1, 2, 3))
      val resultRDD = SimpleApp.doThis(sparkSession)
      assertRDDEquals(expectedRDD, resultRDD)
      //      assertRDDEqualsWithOrder(expectedRDD, resultRDD)
    }

//    it("haha") {
//      //      sparkSession.read.csv("cells.csv")
//      val cells = sparkSession.read.csv("src/test/resources/cells.csv")
//      println("... " + cells.toDF().getClass)
//      println(s"→ 'cells' is a ${cells.getClass}")
//      cells.take(10).map(println)
//    }

  }

}
