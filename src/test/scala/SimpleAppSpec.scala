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

}
