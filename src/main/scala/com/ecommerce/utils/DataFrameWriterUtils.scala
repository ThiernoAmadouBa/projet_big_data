package com.ecommerce.utils

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * Fonctions communes réutilisées par les trois membres (NB de la Question 1.1) :
 *  - écriture CSV + Parquet (une seule implémentation pour toutes les sorties)
 *  - écriture d'un fichier CSV unique (rapport de qualité)
 *  - calcul du taux de rejet
 */
object DataFrameWriterUtils {

  /** Le CSV n'accepte ni tableaux ni structures : on les convertit en texte. */
  def csvSafe(df: DataFrame): DataFrame =
    df.schema.fields.foldLeft(df) { (acc, field) =>
      field.dataType match {
        case _: ArrayType =>
          acc.withColumn(field.name, concat_ws(",", col(field.name).cast("array<string>")))
        case _: StructType | _: MapType =>
          acc.withColumn(field.name, to_json(col(field.name)))
        case _ => acc
      }
    }

  /**
   * Écrit `df` deux fois, en mode overwrite :
   *   <outputPath>/csv/<name>/      (header = true)
   *   <outputPath>/parquet/<name>/
   *
   * @param singleCsvFile true => coalesce(1) (petits résultats) ; false => plusieurs fichiers part-*
   */
  def writeCsvAndParquet(df: DataFrame, name: String, outputPath: String, singleCsvFile: Boolean = true): Unit = {
    val base   = outputPath.stripSuffix("/")
    val csvDf  = csvSafe(df)
    val toCsv  = if (singleCsvFile) csvDf.coalesce(1) else csvDf

    toCsv.write.mode(SaveMode.Overwrite).option("header", "true").csv(s"$base/csv/$name")
    df.write.mode(SaveMode.Overwrite).parquet(s"$base/parquet/$name")
    println(s"[SORTIE] '$name' écrit dans $base/csv/$name et $base/parquet/$name")
  }

  /**
   * Écrit un DataFrame dans UN SEUL fichier CSV nommé `fileName` directement dans `outputDir` :
   * coalesce(1) dans un dossier temporaire, puis renommage du part-*.csv.
   */
  def writeSingleCsvFile(df: DataFrame, outputDir: String, fileName: String, sep: String = ";"): Unit = {
    val base   = outputDir.stripSuffix("/")
    val tmpDir = s"$base/_tmp_$fileName"

    csvSafe(df).coalesce(1).write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("sep", sep)
      .csv(tmpDir)

    val hadoopConf = df.sparkSession.sparkContext.hadoopConfiguration
    val tmpPath    = new Path(tmpDir)
    val fs         = tmpPath.getFileSystem(hadoopConf)
    val parts      = Option(fs.globStatus(new Path(tmpDir, "part-*.csv"))).getOrElse(Array.empty)
    val target     = new Path(s"$base/$fileName")

    parts.headOption.foreach { part =>
      if (fs.exists(target)) fs.delete(target, false)
      fs.rename(part.getPath, target)
    }
    fs.delete(tmpPath, true)
    println(s"[SORTIE] fichier unique écrit : $base/$fileName")
  }

  /** Taux de rejet en % arrondi à 2 décimales. */
  def rejectionRate(linesRead: Long, linesRejected: Long): Double =
    if (linesRead == 0L) 0.0
    else BigDecimal(linesRejected.toDouble / linesRead * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
}
