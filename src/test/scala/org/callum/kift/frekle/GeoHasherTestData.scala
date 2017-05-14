package org.callum.kift

import org.apache.spark.SparkContext
import ch.hsr.geohash.BoundingBox
import org.apache.spark.sql.SparkSession

/**
  * Created by callumkift on 12/05/2017.
  */
package object GeoHasherTestData {

  def dataFrameFixture(sc: SparkContext) = new {
    val sparkSession = SparkSession.builder().getOrCreate()

    import sparkSession.sqlContext.implicits._

    private val data = Seq(
      (12.1, 3.24, "foo"),
      (-32.0, 35.1, "bar"),
      (-12.9, -176.2, "foo"),
      (-12.9, -176.2, "bar"),
      (54.9, 44.4, "foo")
    )

    val df = sc.parallelize(data).toDF("lat", "lng", "doNotCare")
  }

  def boundingBoxFixture() = new {

    val bb1 = new BoundingBox(1, 2, 1, 2)
    val bb2 = new BoundingBox(-1, 42, 134.435, 0)

  }

}
