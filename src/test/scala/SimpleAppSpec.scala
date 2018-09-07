import SimpleApp.CDR
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, RDDComparisons}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}
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

    //  implicit def bool2int(b:Boolean) = if (b) 1 else 0
    implicit def string2Bool(i: String): Boolean = i == "1"

    it("osef") {

      import org.apache.spark.sql.Encoders
      import sqlContext.implicits._

      val ee = Encoders.product[CDR]
      println(s"→→ ${ee}")
      println(s"→→ ${ee.clsTag}")
      println(s"→→ ${ee.schema}")


      //      val cellsDF = sparkSession.read
      //        .option("header", "true")
      //        .csv("/enc/home/miguel/documents/it/spark/riaktr/src/test/resources/cells.csv")
      //        .as[Cell]
      val cdrDF = sparkSession.read
        .option("header", "true")
        .schema(Encoders.product[CDR].schema)
        .csv("/enc/home/miguel/documents/it/spark/riaktr/src/test/resources/cdrs.csv")
        .as[CDR]

      val expectedDF = sparkSession.sparkContext
        .parallelize(Seq("A3245", "A2153", "A9481"))
        .toDF("cell_id")

//      assertDataFrameEquals(expectedDF, SimpleApp.mostUsedCells(cdrDF))
//      println(s"→→→ ${cdrDF.schema}")
//      cdrDF.show
//      cdrDF.filter(_.dropped > 0).show()
//      cdrDF.printSchema()

    }

  }

}
