package org.callum.kift.freckle

import java.io.File
import java.nio.file.{Files, Paths}

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by callumkift on 14/05/2017.
  */
object Freckle {

  def localPathExists(path: String): Boolean = Files.exists(Paths.get(path))

  def dataReader(dir: String): DataFrame = {
    val sqlContext = SparkSession.builder().getOrCreate()

    dir match {
      case x if x.endsWith("/") => sqlContext.read.json(x.concat("*"))
      case y if y.endsWith("/*") => sqlContext.read.json(y)
      case z => sqlContext.read.json(z.concat("/*"))
    }
  }

  def dataWriter(df: DataFrame, path: Option[String]) = path match {
    case None => println("Not saving file")
    case Some(p) =>
      df.write.parquet(p)
      println(s"Rasults saved to ${path}")
  }

  def showDF(df: DataFrame, orderBy: Option[String] = None, n: Int = 5): Unit = orderBy match {
    case None => df.show(n, truncate = false)
    case Some(col) => df.orderBy(functions.desc(col)).show(n, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    case class ArgsConfig(
      func: Option[String] = None,
      inputDirectory: Option[String] = None,
      outputDirectory: Option[String] = None,
      configPath: Option[String] = None)

    val possibleFunctions = Seq("stats", "geohash", "cluster")

    val parser = new scopt.OptionParser[ArgsConfig]("") {
      opt[String]('f', "func").required()
        .action((x, c) => c.copy(func = Some(x)))
        .text(s"REQUIRED: func is the function name")
        .validate(
          x =>
            if (possibleFunctions.contains(x)) success
            else failure(s"func should be one of the following: ${possibleFunctions.mkString(", ")}"))

      opt[String]('i', "inputDirectory").required()
        .action((x, c) => c.copy(inputDirectory = Some(x)))
        .text("REQUIRED: The directory path to the input data")
        .validate(
          x =>
            if (localPathExists(x)) success
            else failure(s"Path does not exist: ${x}"))

      opt[String]('o', "outputDirectory").optional()
        .action((x, c) => c.copy(outputDirectory = Some(x)))
        .text("OPTIONAL: Directory to save output files. If not given, does not save file.")
        .validate(
          x => if (!localPathExists(x)) success
          else failure(s"${x} already exists"))

      opt[String]('c', "conf").optional()
        .action((x, c) => c.copy(configPath = Some(x)))
        .text("OPTIONAL: Path to config. If not given, uses conf in resources")
        .validate(
          x => if (localPathExists(x)) success
          else failure(s"${x} already exists"))

      help("help").text("prints this usage text")

      note(
        "\nSome notes.\n" +
          s" --func takes the values: ${possibleFunctions.mkString(", ")}\n" +
          s"stats:- gets the max, min, avg, std dev for the location-events per IDFA\n" +
          s"geohash:- hashes the coordinates of each event\n" +
          s"cluster:- finds clusters of the people in a hashed location"
      )
    }


    parser.parse(args, ArgsConfig()) match {
      case Some(argsConfig) =>

        val config = argsConfig.configPath match {
          case None => ConfigFactory.load()
          case Some(path) => ConfigFactory.parseFile(new File(path))
        }

        val sparkConf = new SparkConf().setAppName("Freckle").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        val sqlContext = SparkSession.builder().getOrCreate()

        val df = dataReader(argsConfig.inputDirectory.get)

        argsConfig.func.get match {
          case "stats" =>
            val timeGrouping = config.getString("stats.timeGrouping")
            val statsDF = SimpleStats
              .dataframeSimpleStats(
                df,
                config.getString("stats.groupOn"),
                config.getString("stats.timeCol"),
                timeGrouping)
            showDF(statsDF, Some(s"max-${timeGrouping}"))
            dataWriter(statsDF, argsConfig.outputDirectory)

          case "geohash" =>
            val geoHashDF = GeoHasher
              .coordinateHasher(
                sqlContext,
                df,
                config.getString("hashCluster.latCol"),
                config.getString("hashCluster.lngCol"),
                config.getInt("hashCluster.hashLength"))
            showDF(geoHashDF)
            dataWriter(geoHashDF, argsConfig.outputDirectory)
          case "cluster" =>
            val clusterDF = GeoHasher
              .findClusters(
                sqlContext,
                df,
                config.getString("hashCluster.latCol"),
                config.getString("hashCluster.lngCol"),
                config.getInt("hashCluster.hashLength"))
            showDF(clusterDF, Some("count"))
            dataWriter(clusterDF, argsConfig.outputDirectory)
        }

        sc.stop()

      case None => println("Arguments were bad, please try something different")
    }

  }

}
