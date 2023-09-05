name := "AlphaLakeHouse"

version := "0.1"

scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1",
  "org.apache.spark" %% "spark-sql" % "3.4.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.4.1",
  "io.delta" %% "delta-core" % "1.0.0", 
  "com.typesafe" % "config" % "1.4.1" 
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
