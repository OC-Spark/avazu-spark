import AssemblyKeys._

name := "spark-examples"

lazy val commonSettings = Seq(
  version := "1.0",
  organization := "com.jcalc",
  scalaVersion := "2.10.4"
)

val sparkVersion = "1.2.0"
val hbaseVersion = "0.98.7-hadoop2"
val hadoopVersion = "2.4.0"

lazy val app = (project in file("."))
  .settings(commonSettings: _*)
  .settings(
    // your settings here
  )

libraryDependencies <<= scalaVersion {
  scala_version => Seq(
    // Spark and Spark Streaming
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-flume" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-mqtt" % sparkVersion,
    "org.apache.spark" %% "spark-streaming-zeromq" % sparkVersion,
    "org.apache.spark" %% "spark-graphx" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion withSources(),
    "org.apache.spark" %% "spark-bagel" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.apache.spark" %% "spark-graphx" % sparkVersion,
    "org.apache.cassandra" % "cassandra-all" % "1.2.6",
    // Kafka
    "org.apache.kafka" %% "kafka" % "0.8.1.1",
     // Apache Commons Lang Utils
    "org.apache.commons" % "commons-lang3" % "3.3.2",  
    "com.github.scopt" %% "scopt" % "3.2.0",  
    "com.twitter" %% "algebird-core" % "0.8.1",  
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
    "org.apache.hbase" % "hbase-annotations" % hbaseVersion,
    "org.apache.hbase" % "hbase-protocol" % hbaseVersion,
    "org.apache.hbase" % "hbase-client" % hbaseVersion,
    "org.apache.hbase" % "hbase-server" % hbaseVersion,
    "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion,
    "org.apache.hbase" % "hbase-common" % hbaseVersion,
    "org.apache.commons" % "commons-math3" %"3.2",
    "commons-lang" % "commons-lang" % "2.6"
  )
}

resolvers += "typesafe repo" at " http://repo.typesafe.com/typesafe/releases/"

EclipseKeys.withSource := true


