name := "frekle-challenge"

version := "0.0.1"

scalaVersion := "2.11.8"

lazy val sparkVersion = "2.1.0"

parallelExecution in Test := false

libraryDependencies := Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "ch.hsr" % "geohash" % "1.3.0",
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.typesafe" % "config" % "1.3.1",

  // Tests
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % "test"
)
    