name := "SparkStockForecast"

version := "1.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(

	  "org.apache.spark" % "spark-core_2.11" % "2.0.0",
	  "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.6.3",
    "org.apache.spark" % "spark-sql_2.11" % "2.0.0",
    "org.apache.spark" % "spark-mllib_2.11" % "2.0.0",
    "org.apache.spark" % "spark-streaming_2.11" % "2.0.0",
    "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.0-M3"
	
)

