import SimpleApp.{CDR, Cell}
import com.holdenkarau.spark.testing.{DatasetSuiteBase, RDDComparisons}
import org.scalactic.TolerantNumerics
import org.scalatest.FunSpec

class SimpleAppSpec
  extends FunSpec
    with DatasetSuiteBase
    with RDDComparisons {

  import sqlContext.implicits._
  implicit val doubleEquality = TolerantNumerics.tolerantDoubleEquality(0.001)

  describe("mostUsedCells") {

    it("should correctly compute the most used cell") {

      val cellDS = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1),
        CDR("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1)
      ).toDS()
      assert("cell_id1" === SimpleApp.mostUsedCell(cellDS))
    }

  }

  describe("distinctCalleeCount") {

    it("should detect duplicate callees") {

      val inDF = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1),
        CDR("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1)
      ).toDS()
      assert(2 === SimpleApp.distinctCalleeCount(inDF))
    }

  }

  describe("droppedCallCount") {

    it("should correctly count dropped calls") {

      val inDF = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 1),
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1),
        CDR("A3241", "callee_id20", "cell_id2", 122.4, "type2", 1)
      ).toDS()
      assert(Map("A3245" -> 1, "A3241" -> 2) === SimpleApp.droppedCallCountPerCaller(inDF))
    }

  }

  describe("totalCallDuration") {

    it("should correctly compute the total call duration") {

      val inDF = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id1", 3.2, "type1", 0),
        CDR("A3241", "callee_id20", "cell_id2", 3.7, "type2", 1),
        CDR("A3241", "callee_id20", "cell_id2", 2.7, "type2", 1),
        CDR("A3241", "callee_id20", "cell_id2", 3.9, "type2", 1),
        CDR("A3241", "callee_id20", "cell_id2", 3.1, "type2", 1)
      ).toDS()
      assert(Map("A3245" -> 4.4, "A3241" -> 13.4) === SimpleApp.totalCallDurationPerCaller(inDF))
    }

  }

  describe("internationalCallDuration") {

    it("should correctly compute the international call duration") {

      val inDF = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "on-net", 0),
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "international", 0),
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "off-net", 0),
        CDR("A3245", "callee_id1", "cell_id1", 2.3, "international", 0),
        CDR("A3241", "callee_id20", "cell_id2", 3.4, "international", 1)
      ).toDS()
      assert(Map("A3245" -> 3.5, "A3241" -> 3.4) === SimpleApp.internationalCallDurationPerCaller(inDF))
    }

  }

  describe("onNetCallAverageDuration") {

    it("should correctly compute the on-net call average duration") {

      val cdrDS = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 0.9, "on-net", 0),
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "international", 0),
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "off-net", 0),
        CDR("A3245", "callee_id1", "cell_id1", 2.3, "on-net", 0),
        CDR("A3241", "callee_id20", "cell_id2", 3.4, "on-net", 1)
      ).toDS()
      assert(2.2 === SimpleApp.onNetCallAverageDuration(cdrDS))
    }

  }

  describe("lessThan10minCallCount") {

    it("should correctly compute the count of calls that lasted less than 10'") {

      val cdrDS = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 0.9, "on-net", 0),
        CDR("A3245", "callee_id1", "cell_id1", 10.2, "international", 0),
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "off-net", 0),
        CDR("A3245", "callee_id1", "cell_id1", 20.3, "on-net", 0),
        CDR("A3241", "callee_id20", "cell_id2", 3.4, "on-net", 1)
      ).toDS()
      assert(3 === SimpleApp.lessThan10minCallCount(cdrDS))
    }

  }

  describe("top3CalleIds") {

    it("should correctly compute the top 3 calle Ids") {

      val cdrDS = Seq(
        //  Caller ID, Callee ID, Cell ID, Dur., Type,     dropped
        CDR("1538257", "4623421", "A3245", 23.4, "on-net", 0),
        CDR("1538257", "4623421", "A3245", 23.4, "on-net", 0),
        CDR("4123564", "1493853", "A2153", 12.1, "off-net", 0),
        CDR("4123564", "1493853", "A2153", 12.1, "off-net", 0),
        CDR("1535123", "6123138", "A9481", 3.2," international", 0),
        CDR("1538257", "4623421", "A5847", 23.4, "on-net", 1),
        CDR("1538257", "5253463", "A3245", 3.8, "on-net", 0),
        CDR("4123564", "1493853", "A2153", 12.1, "off-net", 1),
        CDR("5283852", "6123138", "A3271", 6.3, "on-net", 0),
        CDR("4123564", "1493853", "A2153", 12.1, "off-net", 0),
        CDR("1538257", "4124566", "A3245", 43.7, "international", 0)
      ).toDS()

      val cellDS = Seq(
        Cell("A3245", 4.392824951181683, 50.794954017278855),
        Cell("A2153", 4.39383786825585, 50.79807518156911),
        Cell("A9481", 4.40814532192845, 50.79519411424009)
      ).toDS()

      assert(Seq("1493853", "4623421", "6123138") === SimpleApp.top3CalleeIds(cdrDS))
    }

  }
}
