name := "Spark Streaming"
version := "0.1"
scalaVersion := "2.11.12"

resolvers := List("Hortonworks Releases" at "https://repo.hortonworks.com/content/repositories/releases/")

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test"

libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.1.3.0.1.0-187"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.1.3.0.1.0-187"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.1.1.3.0.1.0-187"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1.3.0.1.0-187"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1.3.0.1.0-187"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1.3.0.1.0-187"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1.3.0.1.0-187"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1.3.0.1.0-187"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1.3.0.1.0-187"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1.3.0.1.0-187"
libraryDependencies += "org.json4s" %% "json4s-core" % "3.2.11"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.11"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "2.0.0.3.0.1.0-187"
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
libraryDependencies += "org.apache.hbase" % "hbase-spark" % "2.0.0.3.0.1.0-187"

mainClass in (Compile, run) := Some("com.cirrus.training.DataProducer")
mainClass in (Compile, run) := Some("com.cirrus.training.DataConsumer")
