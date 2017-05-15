package org.callum.kift.freckle

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.callum.kift.SimpleStatsTestData.simpleStatsFixture
import org.callum.kift.freckle.SimpleStats.dataframeSimpleStats
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by callumkift on 12/05/2017.
  */
class SimpleStatsTest extends FlatSpec with Matchers with SharedSparkContext {

  "dataframeSimpleStats" should "should throw an IllegalArgumentException with an incorrect timeGrouping" in {

    val f = simpleStatsFixture(sc)
    import f.df

    an[IllegalArgumentException] shouldBe thrownBy(dataframeSimpleStats(df, "name", "timeColumn", "monthly"))

  }

  it should "should throw an AssertionError with an incorrect input column" in {

    val f = simpleStatsFixture(sc)
    import f.df

    an[AssertionError] shouldBe thrownBy(dataframeSimpleStats(df, "wrongName", "timeColumn"))
    an[AssertionError] shouldBe thrownBy(dataframeSimpleStats(df, "name", "wrongTime"))

  }

  it should "create stats per name per second" in {

    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.sqlContext.implicits._

    val f = simpleStatsFixture(sc)
    import f.df

    val timeGrouping = "second"
    val output = dataframeSimpleStats(df, "name", "timeColumn", timeGrouping)

    val expectedData = Seq(
      ("a", 2, 1, 1.25, 0.49999999999999994),
      ("b", 1, 1, 1.0, 0.0),
      ("c", 1, 1, 1.0, 0.0)
    )

    val statColNames = Seq("max", "min", "avg", "stddev").map(_.concat(s"-${timeGrouping}"))
    val colNames = Seq("name") ++ statColNames

    val expected = sc.parallelize(expectedData).toDF(colNames: _*)

    output.collect() should contain theSameElementsAs expected.collect()

  }


  it should "create stats per name per minute" in {

    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.sqlContext.implicits._

    val f = simpleStatsFixture(sc)
    import f.df

    val timeGrouping = "minute"
    val output = dataframeSimpleStats(df, "name", "timeColumn", timeGrouping)

    val expectedData = Seq(
      ("a", 3, 1, 1.6666666666666667, 1.1547005383792515),
      ("b", 1, 1, 1.0, 0.0),
      ("c", 1, 1, 1.0, 0.0)
    )

    val statColNames = Seq("max", "min", "avg", "stddev").map(_.concat(s"-${timeGrouping}"))
    val colNames = Seq("name") ++ statColNames

    val expected = sc.parallelize(expectedData).toDF(colNames: _*)

    output.collect() should contain theSameElementsAs expected.collect()

  }

}
