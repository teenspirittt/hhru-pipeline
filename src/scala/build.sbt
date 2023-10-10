name := "HRActivityAnalysis"

version := "1.0"

scalaVersion := "2.12.18"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.scala-lang.modules" %% "scala-xml" % "2.0.1",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.json4s" %% "json4s-jackson" % "3.6.7",
  "org.json4s" %% "json4s-native" % "3.6.7"
)

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.0.1"
