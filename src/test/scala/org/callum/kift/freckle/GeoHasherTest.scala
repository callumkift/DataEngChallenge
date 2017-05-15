package org.callum.kift.freckle

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.callum.kift.GeoHasherTestData._
import org.callum.kift.freckle.GeoHasher._
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

/**
  * Created by callumkift on 12/05/2017.
  */
class GeoHasherTest extends FlatSpec with Matchers with PrivateMethodTester with SharedSparkContext {

  "coordinateHasher" should "throw an assertion error when an incorrect column is given" in {

    val sparkSession = SparkSession.builder().getOrCreate()

    val f = dataFrameFixture(sc)
    import f.df

    an[AssertionError] shouldBe thrownBy(coordinateHasher(sparkSession, df, "LAT", "lng"))
    an[AssertionError] shouldBe thrownBy(coordinateHasher(sparkSession, df, "lat", "LNG"))
  }

  it should "throw an assertion error when an incorrect hashing length is given" in {

    val sparkSession = SparkSession.builder().getOrCreate()

    val f = dataFrameFixture(sc)
    import f.df

    an[AssertionError] shouldBe thrownBy(coordinateHasher(sparkSession, df, hashLength = 0))
  }

  it should "create a dataframe, geoHash and store useful information about each 'location-event'" in {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val f = dataFrameFixture(sc)
    import f.df

    val output = coordinateHasher(sparkSession, df, hashLength = 9)

    val expectedData = Seq(
      hashPlus(
        "s44kcdxr9",
        12.1,
        3.24,
        hashDimensionsMeters(4.747427646067139, 4.671864399718054),
        latLng(12.100002765655518, 3.2400012016296387),
        0.3327371847619597),
    hashPlus(
      "kf2cywx43",
      -32.0,
      35.1,
      hashDimensionsMeters(4.758745612179408, 4.055205736443609),
      latLng(-32.000019550323486, 35.10000944137573),
      2.3442668453037134),
    hashPlus(
      "2jdww7xh4",
      -12.9,
      -176.2,
      hashDimensionsMeters(4.747709031972007, 4.657518094433559),
      latLng(-12.899987697601318, -176.19999647140503),
      1.4138588514358295),
    hashPlus(
      "2jdww7xh4",
      -12.9,
      -176.2,
      hashDimensionsMeters(4.747709031972007, 4.657518094433559),
      latLng(-12.899987697601318, -176.19999647140503),
      1.4138588514358295),
    hashPlus(
      "ucz8665pd",
      54.9,
      44.4,
      hashDimensionsMeters(4.777408527966347, 2.753151954975031),
      latLng(54.89999055862427, 44.39997911453247),
      1.7029121025840992)
    )

    val expected = sc.parallelize(expectedData).toDF()

    output.collect() should contain theSameElementsAs expected.collect()
  }

  "findClusters" should "find how many people were in that location" in {
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val f = dataFrameFixture(sc)
    import f.df

    val output = findClusters(sparkSession, df, hashLength = 9)

    val expectedData = Seq(
      ("s44kcdxr9",
        latLng(12.100002765655518, 3.2400012016296387),
        hashDimensionsMeters(4.747427646067139, 4.671864399718054),
      1),
      ("kf2cywx43",
        latLng(-32.000019550323486, 35.10000944137573),
        hashDimensionsMeters(4.758745612179408, 4.055205736443609),
      1),
      ("2jdww7xh4",
        latLng(-12.899987697601318, -176.19999647140503),
        hashDimensionsMeters(4.747709031972007, 4.657518094433559),
      2),
      ("ucz8665pd",
        latLng(54.89999055862427, 44.39997911453247),
        hashDimensionsMeters(4.777408527966347, 2.753151954975031),
      1)
    )

    val expected = sc.parallelize(expectedData).toDF("geoHash", "center", "size", "count")

    output.collect() should contain theSameElementsAs expected.collect()

  }

  "boundingBoxSize" should "calculate the area of the GeoHash in meters" in {

    val f = boundingBoxFixture()
    import f._

    val boundingBoxSize = PrivateMethod[hashDimensionsMeters]('boundingBoxSize)

    val output1 = GeoHasher invokePrivate boundingBoxSize(bb1)
    val output2 = GeoHasher invokePrivate boundingBoxSize(bb2)

    val expected1 = hashDimensionsMeters(110575.06481371052, 111252.12979937403)
    val expected2 = hashDimensionsMeters(4762211.268101339, 9642234.236145372)

    output1 shouldBe expected1
    output2 shouldBe expected2

  }

}
