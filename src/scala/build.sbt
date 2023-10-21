name := "HRActivityAnalysis"

version := "1.0"

scalaVersion := "2.12.18"

lazy val akkaVersion = "2.5.32"
// add assembly plugin
//assembly sbt

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0" % Provided,
  "org.scala-lang.modules" %% "scala-xml" % "2.0.1",
  "org.json4s" %% "json4s-jackson" % "3.6.7",
  "org.json4s" %% "json4s-native" % "3.6.7",
)

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.0.1"

