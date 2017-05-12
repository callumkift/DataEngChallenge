package org.callum.kift

import org.apache.spark.SparkContext
import ch.hsr.geohash.BoundingBox

/**
  * Created by callumkift on 12/05/2017.
  */
package object GeoHasherTestData {

  def dataFrameFixture(sc: SparkContext) = ???

  def boundingBoxFixture() = new {

    val bb1 = new BoundingBox(1, 2, 1, 2)
    val bb2 = new BoundingBox(-1, 42, 134.435, 0)

  }

}
