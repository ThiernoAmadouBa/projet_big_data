package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

/** Création centralisée de la SparkSession à partir de l'AppConfig. */
object SparkSessionBuilder {

  def build(cfg: AppConfig): SparkSession = {
    // Windows : Hadoop a besoin de winutils.exe pour écrire des fichiers en local
    if (cfg.hadoopHome.nonEmpty) System.setProperty("hadoop.home.dir", cfg.hadoopHome)

    val builder = SparkSession.builder()
      .appName(cfg.name)
      .config("spark.sql.shuffle.partitions", cfg.shufflePartitions.toString) // Q5.2
      .config("spark.sql.session.timeZone", "UTC")                            // dates identiques sur toutes les machines
      .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")               // horodatage invalide => null (pas d'exception)
      .config("spark.ui.showConsoleProgress", "false")

    // Windows : contourne l'erreur NativeIO$Windows.access0 (pas de winutils.exe / hadoop.dll)
    val isWindows = System.getProperty("os.name", "").toLowerCase.contains("win")
    if (isWindows && cfg.windowsLocalFsFix) {
      builder
        .config("spark.hadoop.fs.file.impl", classOf[WindowsSafeLocalFileSystem].getName)
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    }

    // Sous spark-submit, --master fixe la propriété spark.master : on ne l'écrase pas.
    if (!sys.props.contains("spark.master")) builder.master(cfg.master)

    // Mode "sans optimisation" : on interdit aussi le broadcast automatique de Spark
    if (!cfg.optimization.enableBroadcast) {
      builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")
    }

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
