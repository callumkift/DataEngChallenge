package org.callum.kift.frekle

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import org.callum.kift.GeoHasherTestData._
import org.callum.kift.frekle.GeoHasher.hashDimensionsMeters

/**
  * Created by callumkift on 12/05/2017.
  */
class GeoHasherTest extends FlatSpec with Matchers with PrivateMethodTester with SharedSparkContext{

  "coordinateHasher" should "throw an assertion error when an incorrect column is given" in {

  }

  it should "throw an assertion error when an incorrect hashing length is given" in {

  }

  it should "create a dataframe, geoHash and store useful information about each 'location-event'" in {

  }

  "findClusters" should "find how many people were in that location" in {


  }

  "boundingBoxSize" should "calculate the area of the GeoHash in meters" in {

    val f = boundingBoxFixture()
    import f._

    val boundingBoxSize = PrivateMethod[hashDimensionsMeters]('boundingBoxSize)

    val output1 = GeoHasher invokePrivate boundingBoxSize(bb1)

    val expected1 = hashDimensionsMeters(110575.06481371052, 111252.12979937403)

    output1 shouldBe expected1

  }

}
