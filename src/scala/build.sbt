name := "HRActivityAnalysis"

version := "1.0"

scalaVersion := "2.12.18"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "org.scala-lang.modules" %% "scala-xml" % "2.0.1"
)

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.0.1"
