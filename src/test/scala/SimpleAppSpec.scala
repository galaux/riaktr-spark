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

      val cdrDS = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id2", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id2", 320.2, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id3", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id3", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id3", 121.4, "type1", 0),
        CDR("A3241", "callee_id20", "cell_id30", 122.4, "type2", 1),
        CDR("A3241", "callee_id20", "cell_id30", 122.4, "type2", 1)
      ).toDS()
      assert(Map(
        "A3245" -> ("cell_id2", (2L, 441.6)),
        "A3241" -> ("cell_id30", (2L, 244.8))
      ) === SimpleApp.mostUsedCellByDurationPerCaller(cdrDS))
    }

  }

  describe("mostUsedCellCoordinates") {

    it("should correctly compute the most used cell with its coordinates") {

      val cdrDS = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id2", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id2", 320.2, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id3", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id3", 121.4, "type1", 0),
        CDR("A3245", "callee_id1", "cell_id3", 121.4, "type1", 0),
        CDR("A3241", "callee_id20", "cell_id30", 122.4, "type2", 1),
        CDR("A3241", "callee_id20", "cell_id30", 122.4, "type2", 1)
      ).toDS()
      val cellDS = Seq(
        Cell("cell_id1", 4.392824951181683, 50.794954017278855),
        Cell("cell_id2", 4.39383786825585, 50.79807518156911),
        Cell("cell_id3", 4.40814532192845, 50.79519411424009),
        Cell("cell_id30", 2.40814532192845, 50.79519411424009)
      ).toDS()
      assert(Map(
        "A3245" -> ("cell_id2", 4.39383786825585, 50.79807518156911),
        "A3241" -> ("cell_id30", 2.40814532192845, 50.79519411424009)
      ) === SimpleApp.mostUsedCellCoordinates(cdrDS, cellDS))
    }

  }
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
      val cdrByCaller = SimpleApp.expandColumns(cdrDS).groupBy("caller_id")
      assert(Map("A3245" -> 2, "A3241" -> 3) === SimpleApp.distinctCalleeCountPerCaller(cdrByCaller))
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
      val cdrByCaller = SimpleApp.expandColumns(cdrDS).groupBy("caller_id")
      assert(Map("A3245" -> 1, "A3241" -> 2) === SimpleApp.droppedCallCountPerCaller(cdrByCaller))
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
      val cdrByCaller = SimpleApp.expandColumns(cdrDS).groupBy("caller_id")
      assert(Map("A3245" -> 4.4, "A3241" -> 13.4) === SimpleApp.totalCallDurationPerCaller(cdrByCaller))
    }

  }

  describe("internationalCallDuration") {

    it("should correctly compute the international call duration") {

      val cdrDS = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "on-net", 0),
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "international", 0),
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "off-net", 0),
        CDR("A3245", "callee_id1", "cell_id1", 2.3, "international", 0),
        CDR("A3241", "callee_id20", "cell_id2", 3.4, "international", 1)
      ).toDS()
      val cdrByCaller = SimpleApp.expandColumns(cdrDS).groupBy("caller_id")
      assert(Map("A3245" -> 3.5, "A3241" -> 3.4) === SimpleApp.internationalCallDurationPerCaller(cdrByCaller))
    }

  }

  describe("onNetCallAverageDuration") {

    it("should correctly compute the on-net call average duration") {

      val cdrDS = Seq(
        CDR("A3245", "callee_id1", "cell_id1", 0.7, "on-net", 0),
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "international", 0),
        CDR("A3245", "callee_id1", "cell_id1", 1.2, "off-net", 0),
        CDR("A3245", "callee_id1", "cell_id1", 2.3, "on-net", 0),
        CDR("A3241", "callee_id20", "cell_id2", 3.4, "on-net", 1)
      ).toDS()
      val cdrByCaller = SimpleApp.expandColumns(cdrDS).groupBy("caller_id")
      assert(Map("A3241" -> 3.4, "A3245" -> 1.5) === SimpleApp.onNetCallAverageDurationPerCaller(cdrByCaller))
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
      val cdrByCaller = SimpleApp.expandColumns(cdrDS).groupBy("caller_id")
      assert(Map("A3245" -> 2, "A3241" -> 1) === SimpleApp.lessThan10minCallCountPerCaller(cdrByCaller))
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
        CDR("fromC",   "toW",     "A2153", 12.1, "off-net", 0)
      ).toDS()

      val expected = Map(
        "fromA" -> Seq("toZ", "toY"),
        "fromB" -> Seq("toZ"),
        "fromC" -> Seq("toZ", "toY", "toX")
      )
      assert(expected === SimpleApp.top3CalleeIdsPerCaller(cdrDS))
    }

  }

}
