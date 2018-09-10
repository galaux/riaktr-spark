import SimpleApp.CDR
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import org.apache.spark.sql._
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class SimpleAppSpec
  extends FunSpec
    with DataFrameSuiteBase
    with RDDComparisons
    with BeforeAndAfterEach
{

  var sparkSession: SparkSession = _

  override def beforeEach() {
    sparkSession = SparkSession.builder()
      .appName("udf testings")
      .master("local")
      .getOrCreate()
  }

  override def afterEach() {
    sparkSession.stop()
  }


  describe("distinctCalleeCount") {

    import sqlContext.implicits._

    it("should detect duplicate callees") {

      val inDF = sparkSession.sparkContext
        .parallelize(
          Seq[(String, String, String, Double, String, Int)](
            ("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
            ("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
            ("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
            ("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1),
            ("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1)
          ))
        .toDF("caller_id", "callee_id", "cell_id", "duration", "type", "dropped")
        .as[CDR]

      assert(2 === SimpleApp.distinctCalleeCount(inDF))
    }

  }

  describe("droppedCallCount") {

    import sqlContext.implicits._

    it("should correctly count dropped calls") {

      val inDF = sparkSession.sparkContext
        .parallelize(
          Seq[(String, String, String, Double, String, Int)](
            ("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
            ("A3245", "callee_id1", "cell_id1", 121.4, "type1", 1),
            ("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
            ("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1),
            ("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1)
          ))
        .toDF("caller_id", "callee_id", "cell_id", "duration", "type", "dropped")
        .as[CDR]

      assert(3 === SimpleApp.droppedCallCount(inDF))
    }

  }

  describe("totalCallDuration") {

    import sqlContext.implicits._

    it("should correctly compute the total call duration") {

      val inDF: Dataset[CDR] = sparkSession.sparkContext
        .parallelize(
          Seq[(String, String, String, Double, String, Int)](
            ("A3245", "callee_id1", "cell_id1", 1.2, "type1", 0),
            ("A3241", "callee_id20", "cell_id2", 3.7, "type2", 1)
          ))
        .toDF("caller_id", "callee_id", "cell_id", "duration", "type", "dropped")
        .as[CDR]

      assert(4.9 === SimpleApp.totalCallDuration(inDF))
    }

  }

  describe("internationalCallDuration") {

    import sqlContext.implicits._

    it("should correctly compute the international call duration") {

      val inDF: Dataset[CDR] = sparkSession.sparkContext
        .parallelize(
          Seq[(String, String, String, Double, String, Int)](
            ("A3245", "callee_id1", "cell_id1", 1.2, "on-net", 0),
            ("A3245", "callee_id1", "cell_id1", 1.2, "international", 0),
            ("A3245", "callee_id1", "cell_id1", 1.2, "off-net", 0),
            ("A3245", "callee_id1", "cell_id1", 2.3, "international", 0),
            ("A3241", "callee_id20", "cell_id2", 3.4, "international", 1)
          ))
        .toDF("caller_id", "callee_id", "cell_id", "duration", "type", "dropped")
        .as[CDR]

      assert(6.9 === SimpleApp.internationalCallDuration(inDF))
    }

  }
}
