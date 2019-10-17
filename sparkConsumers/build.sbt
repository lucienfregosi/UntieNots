name := "scala-kafka-spark-stream-dataframe"

version := "1.0"
val sparkVersion= "2.2.1"
scalaVersion := "2.11.8"
libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion,
   "org.apache.spark" %% "spark-sql"  % sparkVersion,
   "org.apache.spark" % "spark-sql-kafka-0-10_2.11"  % sparkVersion,
   "org.apache.kafka" % "kafka-clients" % "1.0.0",
   "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
   "com.typesafe" % "config" % "1.2.0"

)






