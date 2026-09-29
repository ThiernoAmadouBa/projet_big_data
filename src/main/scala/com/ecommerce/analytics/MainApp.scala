package com.ecommerce.analytics

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.ecommerce.utils._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.control.NonFatal

/**
 * Orchestrateur du pipeline (Membre C - Question 6.1) :
 * ingestion -> validation -> transformation -> analytique -> écriture.
 *
 * @param stage "ingestion", "transformation", "analytics" ou "all" (Question 6.2)
 */
object EcommerceAnalyticsApp {

  private def stageLevel(stage: String): Int = stage match {
    case "ingestion"      => 1
    case "transformation" => 2
    case _                => 3 // analytics et all
  }

  def run(spark: SparkSession, cfg: AppConfig, stage: String): Unit = {
    import spark.implicits._

    val level = stageLevel(stage)
    val timer = new PipelineTimer
    val out   = cfg.outputPath
    val cache = cfg.optimization.enableCache
    val mode  = if (cfg.optimization.enableCache || cfg.optimization.enableBroadcast) "optimise" else "baseline"

    println("=" * 70)
    println(s"  ${cfg.name} - étape : $stage - mode : $mode")
    println(s"  cache = ${cfg.optimization.enableCache} | broadcast = ${cfg.optimization.enableBroadcast} " +
      s"| shuffle.partitions = ${cfg.shufflePartitions}")
    println("=" * 70)

    // ================= 1) INGESTION + VALIDATION =================
    val validated = timer.time("ingestion") {
      val raw = new DataIngestion(spark, cfg).readAll()
      DataValidation.validateAll(raw, cfg)
    }

    println("\n===== RAPPORT DE QUALITÉ DES DONNÉES =====")
    validated.qualityReport.show(truncate = false)

    timer.time("ecriture") {
      val date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))
      DataFrameWriterUtils.writeSingleCsvFile(validated.qualityReport, out, s"rapport_qualite_$date.csv")
      DataFrameWriterUtils.writeCsvAndParquet(validated.rejTransactions, "rejets_transactions", out)
      DataFrameWriterUtils.writeCsvAndParquet(validated.rejUsers, "rejets_users", out)
      DataFrameWriterUtils.writeCsvAndParquet(validated.rejProducts, "rejets_products", out)
      DataFrameWriterUtils.writeCsvAndParquet(validated.rejMerchants, "rejets_merchants", out)
    }
    println("\n----- Exemple de transactions rejetées -----")
    validated.rejTransactions.show(5, truncate = false)

    // Tout ce qui est mis en cache est mémorisé ici pour être libéré à la fin (unpersist)
    val persisted = scala.collection.mutable.ArrayBuffer[org.apache.spark.sql.Dataset[_]](
      validated.transactions, validated.users, validated.products, validated.merchants)

    // ================= 2) TRANSFORMATION =================
    if (level >= 2) {
      val transfo = new DataTransformation(cfg)

      val full = timer.time("transformation") {
        val e1 = transfo.enrichTransactionData(
          validated.transactions, validated.users, validated.products, validated.merchants)
        val e2 = transfo.addBehaviorFeatures(e1)
        val e3 = transfo.addSuspiciousFeatures(e2)
        // Gros DataFrame réutilisé plusieurs fois : stockage sérialisé mémoire + disque (Q5.1)
        val p = SparkOptimizations.persistSerIfEnabled(e3, cache)
        println(f"[TRANSFORMATION] transactions enrichies : ${p.count()}%,d lignes")
        p
      }
      persisted += full

      val enriched = transfo.toEnrichedOutput(full)
      println("\n===== TRANSACTIONS ENRICHIES (extrait) =====")
      enriched.select("transaction_id", "user_id", "amount", "age_group", "day_period",
        "transaction_rank", "total_transactions_user", "montant_cumule_7j", "is_active_user",
        "jours_depuis_achat_precedent").show(10, truncate = false)

      val suspicious = transfo.suspiciousTransactions(full)
      println(s"\n===== TRANSACTIONS SUSPECTES : ${suspicious.count()} détectées =====")
      println(s"Top ${cfg.analytics.suspiciousTopN} des montants les plus élevés :")
      suspicious.select("transaction_id", "user_id", "amount", "ecart_panier_moyen_pct",
        "day_period", "payment_method", "nb_conditions")
        .show(cfg.analytics.suspiciousTopN, truncate = false)

      timer.time("ecriture") {
        DataFrameWriterUtils.writeCsvAndParquet(enriched, "transactions_enrichies", out, singleCsvFile = false)
        DataFrameWriterUtils.writeCsvAndParquet(suspicious, "transactions_suspectes", out)
      }

      // ================= 3) ANALYTIQUE =================
      if (level >= 3) {
        val analytics = new Analytics(cfg)

        val results: Seq[(String, DataFrame)] = timer.time("analytique") {
          val kpi        = analytics.kpiMarchands(enriched)
          val retention  = analytics.cohortRetention(enriched)
          val best       = analytics.bestCohort(retention)
          val rfm        = analytics.rfmClients(enriched)
          val rfmCross   = analytics.rfmCrossCustomerSegment(rfm)
          val top10      = analytics.top10Produits(enriched)
          val caCatReg   = analytics.caCategorieRegion(enriched)
          val caPayment  = analytics.caPaiementPeriode(enriched)

          val named = Seq(
            "kpi_marchands"          -> kpi,
            "cohortes_retention"     -> retention,
            "meilleure_cohorte_3m"   -> best,
            "rfm_clients"            -> rfm,
            "rfm_x_customer_segment" -> rfmCross,
            "top10_produits"         -> top10,
            "ca_categorie_region"    -> caCatReg,
            "ca_paiement_periode"    -> caPayment
          ).map { case (n, df) => (n, SparkOptimizations.cacheIfEnabled(df, cache)) }

          // On force le calcul ici pour mesurer l'étape "analytique"
          named.foreach { case (n, df) => println(f"[ANALYTIQUE] $n%-24s : ${df.count()}%,d lignes") }
          named
        }
        persisted ++= results.map(_._2)

        results.foreach { case (name, df) =>
          println(s"\n===== $name =====")
          df.show(20, truncate = false)
        }

        timer.time("ecriture") {
          results.foreach { case (name, df) => DataFrameWriterUtils.writeCsvAndParquet(df, name, out) }
        }
      }
    }

    // ================= 4) CHRONOMÉTRAGE =================
    println("\n===== DURÉES PAR ÉTAPE =====")
    timer.summary.foreach { case (step, ms) => println(f"  $step%-15s : $ms%,d ms") }
    println(f"  TOTAL           : ${timer.totalMs}%,d ms")
    val timings = (timer.summary :+ ("total" -> timer.totalMs)).toDF("etape", "duree_ms")
    DataFrameWriterUtils.writeSingleCsvFile(timings, out, s"chronometrage_${mode}_$stage.csv")

    // Libération explicite du cache (Q5.1)
    SparkOptimizations.release(persisted: _*)
    println("\n[FIN] Pipeline terminé avec succès.")
  }
}

/** Point d'entrée : spark-submit --class com.ecommerce.analytics.MainApp app.jar [etape] [--no-optim] */
object MainApp {

  private val ValidStages = Seq("ingestion", "transformation", "analytics", "all")

  private def printHelp(bad: String): Unit = {
    println(s"Argument inconnu : '$bad'")
    println(
      """|
         |Usage : MainApp [etape] [--no-optim]
         |
         |Etapes acceptées :
         |  ingestion       lecture + validation + rapport de qualité + rejets
         |  transformation  ingestion + enrichissement (jointures, UDF, fenêtres, suspects)
         |  analytics       pipeline complet jusqu'aux KPI, cohortes, RFM, produits
         |  all             tout le pipeline (valeur par défaut)
         |
         |Option :
         |  --no-optim      désactive cache et broadcast (mesure "avant optimisation", Q5.3)
         |""".stripMargin)
  }

  def main(args: Array[String]): Unit = {
    val positional = args.filterNot(_.startsWith("--"))
    val stage      = positional.headOption.map(_.toLowerCase).getOrElse("all")
    val noOptim    = args.contains("--no-optim")

    if (!ValidStages.contains(stage)) {
      printHelp(stage)
    } else {
      var spark: SparkSession = null
      var failed = false
      try {
        val baseCfg = ConfigLoader.load()
        val cfg =
          if (noOptim) baseCfg.copy(optimization = OptimizationConfig(enableCache = false, enableBroadcast = false))
          else baseCfg
        spark = SparkSessionBuilder.build(cfg)
        EcommerceAnalyticsApp.run(spark, cfg, stage)
      } catch {
        case e: DataIngestionException =>
          failed = true
          System.err.println(s"[ECHEC] ${e.getMessage}")
        case NonFatal(e) =>
          failed = true
          System.err.println(s"[ECHEC] Erreur inattendue : ${e.getMessage}")
          e.printStackTrace()
      } finally {
        // Arrêt propre de la SparkSession quoi qu'il arrive
        if (spark != null) spark.stop()
      }
      if (failed) sys.exit(1)
    }
  }
}
