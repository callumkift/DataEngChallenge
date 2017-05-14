package org.callum.kift.freckle


import ch.hsr.geohash.util.VincentyGeodesy.distanceInMeters
import ch.hsr.geohash.{BoundingBox, GeoHash, WGS84Point}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by callumkift on 12/05/2017.
  */
object GeoHasher {

  case class hashDimensionsMeters(latLength: Double, lngLength: Double)

  case class hashPlus(
    geoHash: String,
    lat: Double,
    lng: Double,
    size: hashDimensionsMeters,
    center: latLng,
    distanceToCenter: Double)

  case class latLng(lat: Double, lng: Double) {
    def geoHash(length: Int): String = GeoHash.withCharacterPrecision(this.lat, this.lng, length).toBase32

    def toWGS84Point = new WGS84Point(this.lat, this.lng)

    def geoHashPlus(length: Int): hashPlus = {
      val lat = this.lat
      val lng = this.lng
      val hash = GeoHash.withCharacterPrecision(lat, lng, length)
      val boundingBox = hash.getBoundingBox
      val centerPoint = boundingBox.getCenterPoint
      hashPlus(
        hash.toBase32,
        lat,
        lng,
        boundingBoxSize(boundingBox),
        latLng(centerPoint.getLatitude, centerPoint.getLongitude),
        distanceInMeters(this.toWGS84Point, centerPoint))
    }

  }

  private def boundingBoxSize(boundingBox: BoundingBox): hashDimensionsMeters = {
    val upperLeft = boundingBox.getUpperLeft
    val lowerRight = boundingBox.getLowerRight
    val upperRight = new WGS84Point(boundingBox.getMaxLat, boundingBox.getMaxLon)

    val latLength = distanceInMeters(lowerRight, upperRight)
    val lngLength = distanceInMeters(upperLeft, upperRight)

    hashDimensionsMeters(latLength, lngLength)
  }

  def coordinateHasher(
    sparkSession: SparkSession,
    df: DataFrame,
    latCol: String = "lat",
    lngCol: String = "lng",
    hashLength: Int = 12): DataFrame = {

    import sparkSession.implicits._
    val dataframeColumns = df.columns.toSet

    assert(dataframeColumns contains latCol, s"Dataframe did not contain ${latCol}")
    assert(dataframeColumns contains lngCol, s"Dataframe did not contain ${lngCol}")
    assert(hashLength > 0, s"hashLength must be between 1 and 12 inclusive.")


    df
      .select(latCol, lngCol).as[latLng]
      .map(_.geoHashPlus(hashLength))
      .toDF()
  }

  def findClusters(
    sparkSession: SparkSession,
    df: DataFrame,
    latCol: String = "lat",
    lngCol: String = "lng",
    hashLength: Int = 12) =
    coordinateHasher(sparkSession, df, latCol, lngCol, hashLength)
      .groupBy("geoHash", "center", "size").count()

}

