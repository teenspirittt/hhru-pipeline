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
  "com.typesafe.akka" %% "akka-http" % "10.2.7",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.2.7", 
  "com.typesafe.akka" %% "akka-stream" % akkaVersion, 
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion, 
  "com.typesafe.akka" %% "akka-http-testkit" % "10.2.7" % Test, 
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test, 
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test 
)

dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "2.0.1"

