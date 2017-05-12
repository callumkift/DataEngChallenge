package org.callum.kift.frekle

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by callumkift on 12/05/2017.
  */
object SimpleStats {

  def dataframeSimpleStats(
    df: DataFrame,
    groupCol: String,
    timeCol: String,
    timeGrouping: String = "second"): DataFrame = {


    val dataframeColumns = df.columns.toSet

    assert(dataframeColumns contains groupCol, s"The dataframe does not have the column ${groupCol}")
    assert(dataframeColumns contains timeCol, s"The dataframe does not have the column ${timeCol}")

    val timeDivisor = timeGrouping match {
      case "second" => 1
      case "minute" => 60
      case "hour" => 60 * 60
      case "day" => 60 * 60 * 24
      case "week" => 60 * 60 * 24 * 7
      case _ => throw new IllegalArgumentException(
        "Time grouping should be one of the following: second, minute, hour, day, week")
    }

    val timeConverter: Long => Long = x => x / timeDivisor
    val timeConverterUDF = udf(timeConverter)

    df
      .withColumn("groupedTime", timeConverterUDF(col(timeCol)))
      .groupBy(groupCol, "groupedTime").count()
      .groupBy(groupCol)
      .agg(
        max("count").as(s"max-${timeGrouping}"),
        min("count").as(s"min-${timeGrouping}"),
        avg("count").as(s"avg-${timeGrouping}"),
        stddev("count").as(s"stddev-${timeGrouping}"))

    // N.B. stddev is the corrected-sample standard deviation and so divides by (N - 1) instead of N.
  }

}
