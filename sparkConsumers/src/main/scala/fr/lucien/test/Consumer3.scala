package fr.lucien.test

import org.apache.spark.sql.functions._

object Consumer3 extends  App {

 import spark.implicits._

 val dfTheme = spark
   .read
   .parquet("src/resources/data/parquet/theme")
   // Source | Thème


  val dfThemes = spark
    .read
    .parquet("src/resources/data/parquet/themes")
  // Source | Word | Thèmes
  // Pour chaque thème les sources qui lui sont associées avec le nombre d'occurrences de chaque mot clé.

   val dfUniqueTheme = dfTheme.join(dfThemes, Seq("source"), "inner")
   .groupBy("theme")
   .agg(collect_list("source") as "sources", collect_list("word") as "words" )



 val dfWordOccurence = dfUniqueTheme.withColumn("exploded", explode(col("words")) as "word")
   .groupBy("theme", "word")
   .agg(count("word") as "occurence")
   .withColumn("wordOccurence", struct($"theme",$"occurence") as "wordOccurence" )
   .groupBy("theme")
   .agg(collect_list("wordOccurence") as "wordOccurenceArray")
   .select("theme", "wordOccurenceArray")

 /*les faux positif (sources que les mots clés ont identifié qui n'appartiennent pas au thème)
 => On part du principe qu'une source est rattaché à un thème si X% des mots clés du thème
   s'y trouvent. (X à passer en argument du programme).*/

 val threshold = 50
 val listeningDataDf = spark.read.option("multiLine", true).json("/src/resources/data/listening.json")
 val listeningDataDfClean = listeningDataDf.withColumn("wordTheme", explode(col("words")))
   .select("theme","wordTheme")

 val dfFauxPositif = dfTheme.join(dfThemes, Seq("source"), "inner")
   .groupBy("theme","source")
   .agg(collect_set("word") as "wordsSource")
   .join(listeningDataDf, Seq("theme"), "inner")
   .withColumn("wordSourceLength", size($"wordsSource"))
   .withColumn("wordThemeLength", size($"wordTheme"))
   .filter(($"wordSourceLengthDouble" * 100)  < ("wordThemeLengthDouble" * threshold))
   .groupBy("theme")
   .agg(collect_list("sources") as "sourceFauxPostifArray")
   .select("theme", "sourceFauxPostifArray")


  /*
  la pertinence de chaque mot clé : % apparition dans une source qui correspondait au
thème / % apparition dans une source qui ne correspondait pas au thème / % absence dans
une source qui correspondait au thème
   */

  // theme | sourcesFauxPositif<> | WordOccurence<>

  val dfUnified = dfWordOccurence.join(dfFauxPositif, Seq("theme"))
    .withColumn("wordOccurence", explode($"wordOccurenceArray"))
    .select("theme","sourceFauxPostifArray", "wordOccurence.word")
    .withColumn("wordIsFauxPositif", when($"sourceFauxPostifArray".isNull(), lit(0)).otherwise(lit(1)))

 dfUnified.cache()

 val dfWordFauxPositif = dfUnified.filter($"wordIsFauxPositif" === 0)
   .groupBy("word")
   .agg(count($"wordIsFauxPositif") as "nbNotFauxPositif")


 val dfWordNotFauxPositif = dfUnified.filter($"wordIsFauxPositif" === 1)
   .groupBy("word")
   .agg(count($"wordIsFauxPositif") as "nbFauxPositif")


 val finalDf = dfWordFauxPositif.join(dfWordNotFauxPositif, Seq("word"),"inner")
   .withColumn("percentApparitionSourceCorrespondate", ($"nbNotFauxPositif" / ($"nbNotFauxPositif" + $"nbFauxPositif")) * 100)
   .withColumn("percentApparitionSourceCorrespondate", ($"nbFauxPositif" / ($"nbNotFauxPositif" + $"nbFauxPositif")) * 100)



}
