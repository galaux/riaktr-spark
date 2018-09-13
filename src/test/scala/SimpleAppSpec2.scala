import SimpleApp.{CDR, Cell}
import com.holdenkarau.spark.testing.{DatasetSuiteBase, RDDComparisons}
import org.scalactic.TolerantNumerics
import org.scalatest.FunSpec

class SimpleAppSpec2
  extends FunSpec
    with DatasetSuiteBase
    with RDDComparisons {

  import sqlContext.implicits._
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.001)


  describe("distinctCalleeCount") {

    it("should detect duplicate callees") {

      val cdrDS = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3245", "callee_id2", "cell_id1", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1),
        CDR("A3241", "callee_id21", "cell_id2", 122.4, "type2", 1),
        CDR("A3241", "callee_id22", "cell_id2", 122.4, "type2", 1)
      ).toDS()
      val result = SimpleApp.allInOne(cdrDS)
      assert(Map("A3245" -> 2, "A3241" -> 3) === result.collect().toMap.mapValues(_.distinctCalleeCount))
    }

  }

    describe("top3CalleeIds") {

      it("should correctly compute the top 3 calle Ids") {

        val cdrDS = Seq(
          //  Caller ID, Callee ID, Cell ID, Dur., Type,     dropped
          CDR("fromA",   "toZ",     "A3245", 23.4, "on-net", 0),
          CDR("fromA",   "toY",     "A3245", 23.4, "on-net", 0),
          //
          CDR("fromB",   "toZ",     "A3245", 23.4, "on-net", 0),
          //
          CDR("fromC",   "toZ",     "A2153", 12.1, "off-net", 0),
          CDR("fromC",   "toZ",     "A2153", 12.1, "off-net", 0),
          CDR("fromC",   "toY",     "A2153", 12.1, "off-net", 0),
          CDR("fromC",   "toY",     "A2153", 12.1, "off-net", 0),
          CDR("fromC",   "toX",     "A2153", 12.1, "off-net", 0),
          CDR("fromC",   "toX",     "A2153", 12.1, "off-net", 0),
          CDR("fromC",   "toX",     "A2153", 12.1, "off-net", 0),
          CDR("fromC",   "toW",     "A2153", 12.1, "off-net", 0)
        ).toDS()

        val expected = Map(
          "fromA" -> Seq("toY", "toZ"),
          "fromB" -> Seq("toZ"),
          "fromC" -> Seq("toX", "toY", "toZ")
        )
        val result = SimpleApp.allInOne(cdrDS)
        assert(expected === result.collect().toMap.mapValues(_.top3Callees))
      }

    }

    describe("droppedCallCount") {

      it("should correctly count dropped calls") {

        val cdrDS = Seq(
          CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
          CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 1),
          CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
          CDR("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1),
          CDR("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1)
        ).toDS()
        val result = SimpleApp.allInOne(cdrDS)
        assert(Map("A3245" -> 1, "A3241" -> 2) === result.collect().toMap.mapValues(_.droppedCallsCount))
      }

    }

    describe("totalCallDuration") {

      it("should correctly compute the total call duration") {

        val cdrDS = Seq(
          CDR("A3245", "callee_id1", "cell_id1", 1.2, "type1", 0),
          CDR("A3245", "callee_id1", "cell_id1", 3.2, "type1", 0),
          CDR("A3241", "callee_id20", "cell_id2", 3.7, "type2", 1),
          CDR("A3241", "callee_id20", "cell_id2", 2.7, "type2", 1),
          CDR("A3241", "callee_id20", "cell_id2", 3.9, "type2", 1),
          CDR("A3241", "callee_id20", "cell_id2", 3.1, "type2", 1)
        ).toDS()
        val result = SimpleApp.allInOne(cdrDS)
        assert(Map("A3245" -> 4.4, "A3241" -> 13.4) === result.collect().toMap.mapValues(_.totalCallDuration))
      }

    }

}
