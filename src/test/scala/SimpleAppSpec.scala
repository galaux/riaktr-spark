import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}
import org.scalatest.FunSpec

class SimpleAppSpec
  extends FunSpec
    with SharedSparkContext
    with RDDComparisons {

  describe("haha") {

    it("osef") {
      val expectedRDD = sc.parallelize(Seq(1, 2, 3))
      val resultRDD = sc.parallelize(Seq(3, 2, 1))
      assertRDDEquals(expectedRDD, resultRDD)
//      assertRDDEqualsWithOrder(expectedRDD, resultRDD)
    }
  }

}
