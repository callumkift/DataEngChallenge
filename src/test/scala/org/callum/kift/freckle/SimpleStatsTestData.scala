package org.callum.kift

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by callumkift on 12/05/2017.
  */
package object SimpleStatsTestData {

  def simpleStatsFixture(sc: SparkContext) = new {

    val sparkSession = SparkSession.builder().getOrCreate()

    import sparkSession.sqlContext.implicits._

    private val data = Seq(
      ("a", 324L, "foo"),
      ("b", 324L, "bar"),
      ("b", 600L, "foo"),
      ("c", 1L, "bar"),
      ("c", 120L, "foo"),
      ("a", 324L, "bar"),
      ("a", 295L, "foo"),
      ("a", 341L, "bar"),
      ("a", 1L, "bar")
    )

    val df = sc.parallelize(data).toDF("name", "timeColumn", "doNotCare")
  }

}
