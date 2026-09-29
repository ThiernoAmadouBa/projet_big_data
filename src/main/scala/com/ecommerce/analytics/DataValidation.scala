package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.{AppConfig, DataFrameWriterUtils, ValidationConfig}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

/** Une ligne du rapport de qualité (Question 2.4 + bonus 2.5). */
case class QualityRow(
  dataset: String,
  nb_lignes_lues: Long,
  nb_lignes_valides: Long,
  nb_lignes_rejetees: Long,
  taux_rejet: Double,
  nb_valeurs_nulles: Long,
  nb_user_id_orphelins: Long,
  nb_product_id_orphelins: Long,
  nb_merchant_id_orphelins: Long
)

/**
 * Validation des données (Membre A - Questions 2.2 à 2.5).
 * Chaque fonction renvoie (lignes valides, lignes rejetées + colonne rejection_reason).
 * Principe : on calcule pour chaque ligne la liste des règles violées avec concat_ws(" | ", ...) ;
 * concat_ws ignore les null => chaîne vide = ligne valide.
 */
object DataValidation {

  val RejectionColumn = "rejection_reason"

  private def split[T](flagged: DataFrame)(implicit enc: Encoder[T]): (Dataset[T], DataFrame) = {
    val valid    = flagged.filter(col(RejectionColumn) === "").drop(RejectionColumn).as[T]
    val rejected = flagged.filter(col(RejectionColumn) =!= "")
    (valid, rejected)
  }

  def validateTransactions(ds: Dataset[Transaction], v: ValidationConfig): (Dataset[Transaction], DataFrame) = {
    import ds.sparkSession.implicits._
    val ts = col("timestamp")
    val flagged = ds.toDF().withColumn(RejectionColumn, concat_ws(" | ",
      when(col("amount").isNull, lit("amount_manquant")),
      when(col("amount") <= v.minAmount, lit("amount <= 0")),
      when(ts.isNull || length(ts) =!= v.timestampLength || !ts.rlike("^[0-9]+$"), lit("timestamp_invalide"))
    ))
    split[Transaction](flagged)
  }

  def validateUsers(ds: Dataset[User], v: ValidationConfig): (Dataset[User], DataFrame) = {
    import ds.sparkSession.implicits._
    val flagged = ds.toDF().withColumn(RejectionColumn, concat_ws(" | ",
      when(col("age").isNull, lit("age_manquant")),
      when(col("age") < v.minAge || col("age") > v.maxAge, lit("age_hors_intervalle")),
      when(col("annual_income").isNull, lit("income_manquant")),
      when(col("annual_income") <= v.minIncome, lit("income <= 0"))
    ))
    split[User](flagged)
  }

  def validateProducts(ds: Dataset[Product], v: ValidationConfig): (Dataset[Product], DataFrame) = {
    import ds.sparkSession.implicits._
    val flagged = ds.toDF().withColumn(RejectionColumn, concat_ws(" | ",
      when(col("price").isNull, lit("price_manquant")),
      when(col("price") <= v.minPrice, lit("price <= 0")),
      when(col("rating").isNull, lit("rating_manquant")),
      when(col("rating") < v.minRating || col("rating") > v.maxRating, lit("rating_hors_intervalle"))
    ))
    split[Product](flagged)
  }

  def validateMerchants(ds: Dataset[Merchant], v: ValidationConfig): (Dataset[Merchant], DataFrame) = {
    import ds.sparkSession.implicits._
    val flagged = ds.toDF().withColumn(RejectionColumn, concat_ws(" | ",
      when(col("commission_rate").isNull, lit("commission_manquante")),
      when(col("commission_rate") < v.minCommission || col("commission_rate") > v.maxCommission,
        lit("commission_hors_intervalle"))
    ))
    split[Merchant](flagged)
  }

  /** Nombre total de valeurs nulles, toutes colonnes confondues (Question 2.4). */
  def countNulls(df: DataFrame): Long = {
    val aggs = df.columns.map(c => sum(when(col(c).isNull, 1L).otherwise(0L)))
    val row  = df.select(aggs: _*).first()
    (0 until row.length).map(i => if (row.isNullAt(i)) 0L else row.getLong(i)).sum
  }

  /** Nombre de lignes de `tx` dont la clé n'existe pas dans `ref` (Question 2.5 - bonus). */
  def countOrphans(tx: DataFrame, ref: DataFrame, key: String): Long =
    tx.select(key).join(ref.select(key).distinct(), Seq(key), "left_anti").count()

  /**
   * Valide les 4 jeux de données, affiche le bilan (Q2.3) et fabrique le rapport de qualité (Q2.4 / Q2.5).
   */
  def validateAll(raw: IngestedData, config: AppConfig): ValidatedData = {
    val spark = raw.transactions.sparkSession
    import spark.implicits._

    val cache = config.optimization.enableCache
    val v     = config.validation

    // Les données brutes sont réutilisées (comptage, nulls, orphelins, validation) => cache (Q5.1)
    val txRaw = SparkOptimizations.cacheIfEnabled(raw.transactions, cache)
    val usRaw = SparkOptimizations.cacheIfEnabled(raw.users, cache)
    val prRaw = SparkOptimizations.cacheIfEnabled(raw.products, cache)
    val meRaw = SparkOptimizations.cacheIfEnabled(raw.merchants, cache)

    val (txOk0, txRej) = validateTransactions(txRaw, v)
    val (usOk0, usRej) = validateUsers(usRaw, v)
    val (prOk0, prRej) = validateProducts(prRaw, v)
    val (meOk0, meRej) = validateMerchants(meRaw, v)

    // Les lignes valides alimentent toute la suite du pipeline => cache
    val txOk = SparkOptimizations.cacheIfEnabled(txOk0, cache)
    val usOk = SparkOptimizations.cacheIfEnabled(usOk0, cache)
    val prOk = SparkOptimizations.cacheIfEnabled(prOk0, cache)
    val meOk = SparkOptimizations.cacheIfEnabled(meOk0, cache)

    // Intégrité référentielle (bonus 2.5) : calculée sur les référentiels lus (avant validation)
    val orphUsers    = countOrphans(txRaw.toDF(), usRaw.toDF(), "user_id")
    val orphProducts = countOrphans(txRaw.toDF(), prRaw.toDF(), "product_id")
    val orphMerch    = countOrphans(txRaw.toDF(), meRaw.toDF(), "merchant_id")

    println("\n[VALIDATION] Bilan avant / après validation")
    def row(name: String, rawDf: DataFrame, validDf: DataFrame, orph: (Long, Long, Long)): QualityRow = {
      val read     = rawDf.count()
      val valid    = validDf.count()
      val rejected = read - valid
      println(f"[VALIDATION] $name%-13s : lues = $read%,d | valides = $valid%,d | rejetées = $rejected%,d")
      QualityRow(name, read, valid, rejected,
        DataFrameWriterUtils.rejectionRate(read, rejected), countNulls(rawDf),
        orph._1, orph._2, orph._3)
    }

    val rows = Seq(
      row("transactions", txRaw.toDF(), txOk.toDF(), (orphUsers, orphProducts, orphMerch)),
      row("users",        usRaw.toDF(), usOk.toDF(), (0L, 0L, 0L)),
      row("products",     prRaw.toDF(), prOk.toDF(), (0L, 0L, 0L)),
      row("merchants",    meRaw.toDF(), meOk.toDF(), (0L, 0L, 0L))
    )
    val report = rows.toDF()

    ValidatedData(txOk, usOk, prOk, meOk, txRej, usRej, prRej, meRej, report)
  }
}
