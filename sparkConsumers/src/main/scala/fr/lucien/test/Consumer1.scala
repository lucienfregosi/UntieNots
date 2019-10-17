package fr.lucien.test

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



object Consumer1 extends  App {
 val logger = LoggerFactory.getLogger(this.getClass)
 import spark.implicits._

 val listeningDataDf = spark.read.option("multiLine", true).json("/src/resources/data/listening.json")


 val listeningDataDfClean = listeningDataDf.withColumn("word", explode(col("words")))
   .select("theme","word")

 val df = spark
   .readStream
   .format("kafka")
   .option("kafka.bootstrap.servers", config.getString("brokers"))
   .option("subscribe", config.getString("topic"))
   .option("startingOffsets", "latest")
   .load()
  

 val dfCommonWords = df.select("value.word", "value.source")
   .join(listeningDataDfClean, Seq("word"), "inner")

 val dfTheme = dfCommonWords.select("source","theme")

 val dfThemes = dfCommonWords.groupBy("source", "word")
   .agg(collect_list("theme") as "themes")
   .select("source","word","themes")

 val dsTheme = dfTheme
   .writeStream
   .format("kafka")
   .option("kafka.bootstrap.servers", config.getString("brokers"))
   .option("topic", "theme")
   .start()

 val dsThemes = dfThemes
   .writeStream
   .format("kafka")
   .option("kafka.bootstrap.servers", config.getString("brokers"))
   .option("topic", "themes")
   .start()

}
