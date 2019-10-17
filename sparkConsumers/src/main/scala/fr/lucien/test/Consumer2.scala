package fr.lucien.test

object Consumer2 extends  App {

 import spark.implicits._



 val dfTheme = spark
   .readStream
   .format("kafka")
   .option("kafka.bootstrap.servers", config.getString("brokers"))
   .option("subscribe", "theme")
   .option("startingOffsets", "latest")
   .load()

 val dfThemes = spark
   .readStream
   .format("kafka")
   .option("kafka.bootstrap.servers", config.getString("brokers"))
   .option("subscribe", "theme")
   .option("startingOffsets", "latest")
   .load()

 dfTheme
   .writeStream
   .outputMode("append")
   .format("parquet")
   .start("src/resources/data/parquet/theme")


 dfThemes
   .writeStream
   .outputMode("append")
   .format("parquet")
   .start("src/resources/data/parquet/themes")

}
