name := "datamarts"

version := "1.0"

scalaVersion := "2.12.10"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.postgresql" % "postgresql" % "9.4.1208"
)

