package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
 * Transformations avancées (Membre B - Partie 3).
 *
 * Chaîne d'appel utilisée par l'application :
 *   enrichTransactionData  (Q3.2 : jointures + UDF + rang + tranche d'âge)
 *   -> addBehaviorFeatures (Q3.3 : fenêtres glissantes, lag)
 *   -> addSuspiciousFeatures (Q3.4 bonus)
 */
class DataTransformation(config: AppConfig) {

  private val broadcastEnabled = config.optimization.enableBroadcast

  /** Broadcast des petites tables (Q5.2), désactivable pour la comparaison avant/après. */
  private def small(df: DataFrame): DataFrame = if (broadcastEnabled) broadcast(df) else df

  // ------------------------------------------------------------------
  //  Question 3.2
  // ------------------------------------------------------------------
  def enrichTransactionData(
    transactions: Dataset[Transaction],
    users: Dataset[User],
    products: Dataset[Product],
    merchants: Dataset[Merchant]
  ): DataFrame = {

    // Tables de dimension : colonnes homonymes renommées, doublons de clé éliminés
    // pour garantir UNE ligne par transaction après jointure.
    val usersDim = users.toDF()
      .select("user_id", "age", "annual_income", "city", "customer_segment")
      .dropDuplicates("user_id")

    val productsDim = products.toDF()
      .select(
        col("product_id"),
        col("name").as("product_name"),
        col("price").as("product_price"),
        col("rating"),
        col("stock"))
      .dropDuplicates("product_id")

    val merchantsDim = merchants.toDF()
      .select(
        col("merchant_id"),
        col("name").as("merchant_name"),
        col("category").as("merchant_category"),
        col("region"),
        col("commission_rate"))
      .dropDuplicates("merchant_id")

    val base = transactions.toDF()
      .withColumn("transaction_date", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))

    // Jointures LEFT : on ne perd aucune transaction valide, même si sa référence est orpheline.
    base
      .join(small(usersDim), Seq("user_id"), "left")
      .join(small(productsDim), Seq("product_id"), "left")
      .join(small(merchantsDim), Seq("merchant_id"), "left")
      // UDF -> struct -> colonnes simples
      .withColumn("time_features", TimeFeatures.extractTimeFeatures(col("timestamp")))
      .select(col("*"), col("time_features.*"))
      .drop("time_features")
      .withColumn("age_group", ageGroup(col("age")))
      .withColumn("transaction_rank",
        row_number().over(Window.partitionBy("user_id").orderBy(col("transaction_date"), col("transaction_id"))))
      .withColumn("total_transactions_user",
        count(lit(1)).over(Window.partitionBy("user_id")))
  }

  /**
   * Tranche d'âge. Le sujet laisse 25 ans hors des intervalles ("moins de 25" puis "26 à 44") :
   * nous rangeons 25 ans dans "Adulte" (Adulte = 25 à 44 ans). Âge inconnu => "Inconnu".
   */
  def ageGroup(age: Column): Column =
    when(age.isNull, lit("Inconnu"))
      .when(age < 25, lit("Jeune"))
      .when(age <= 44, lit("Adulte"))
      .when(age <= 64, lit("Âge Moyen"))
      .otherwise(lit("Senior"))

  // ------------------------------------------------------------------
  //  Question 3.3
  // ------------------------------------------------------------------
  def addBehaviorFeatures(df: DataFrame): DataFrame = {
    val days      = config.analytics.behaviorWindowDays
    val minActive = config.analytics.activeMinDays

    val wUser = Window.partitionBy("user_id").orderBy(col("transaction_date"), col("transaction_id"))
    // Fenêtre glissante sur le temps (en secondes) : [t - 7 jours ; t]
    val w7Seconds = Window.partitionBy("user_id").orderBy(col("_ts")).rangeBetween(-days * 86400L, 0L)
    // Fenêtre glissante sur les jours calendaires : les 7 derniers jours, jour courant inclus
    val w7Days = Window.partitionBy("user_id").orderBy(col("_day")).rangeBetween(-(days - 1).toLong, 0L)

    df
      .withColumn("_ts", col("transaction_date").cast("long"))
      .withColumn("_day", datediff(to_date(col("transaction_date")), to_date(lit("1970-01-01"))))
      // Montant cumulé sur 7 jours glissants
      .withColumn("montant_cumule_7j", round(sum(col("amount")).over(w7Seconds), 2))
      // Utilisateur actif : >= 5 jours distincts avec transaction sur 7 jours glissants
      // (countDistinct est interdit dans une fenêtre => size(collect_set(...)))
      .withColumn("is_active_user",
        when(size(collect_set(col("_day")).over(w7Days)) >= minActive, 1).otherwise(0))
      // Délai (en jours) depuis la transaction précédente du même utilisateur (lag)
      .withColumn("jours_depuis_achat_precedent", col("_day") - lag(col("_day"), 1).over(wUser))
      .drop("_ts", "_day")
  }

  // ------------------------------------------------------------------
  //  Question 3.4 (bonus)
  // ------------------------------------------------------------------
  def addSuspiciousFeatures(df: DataFrame): DataFrame = {
    val a     = config.analytics
    val wUser = Window.partitionBy("user_id").orderBy(col("transaction_date"), col("transaction_id"))
    val wAll  = Window.partitionBy("user_id")

    df
      .withColumn("panier_moyen_user", avg(col("amount")).over(wAll))
      .withColumn("ecart_panier_moyen_pct",
        round((col("amount") - col("panier_moyen_user")) / col("panier_moyen_user") * 100, 2))
      .withColumn("delai_precedente_minutes",
        round((col("transaction_date").cast("long") - lag(col("transaction_date").cast("long"), 1).over(wUser)) / 60.0, 2))
      .withColumn("cond_montant",
        when(col("amount") > col("panier_moyen_user") * (1 + a.suspiciousAmountPct / 100.0), 1).otherwise(0))
      .withColumn("cond_night", when(col("day_period") === "Night", 1).otherwise(0))
      .withColumn("cond_delai",
        when(col("delai_precedente_minutes") < a.suspiciousDelayMinutes, 1).otherwise(0))
      .withColumn("cond_crypto", when(col("payment_method") === "CRYPTO", 1).otherwise(0))
      .withColumn("nb_conditions",
        col("cond_montant") + col("cond_night") + col("cond_delai") + col("cond_crypto"))
      .withColumn("is_suspicious", when(col("nb_conditions") >= a.suspiciousMinConditions, 1).otherwise(0))
  }

  // ------------------------------------------------------------------
  //  Colonnes de sortie
  // ------------------------------------------------------------------
  private val enrichedColumns = Seq(
    "transaction_id", "user_id", "product_id", "merchant_id", "amount", "timestamp", "transaction_date",
    "location", "payment_method", "category",
    "age", "annual_income", "city", "customer_segment", "age_group",
    "product_name", "product_price", "rating", "stock",
    "merchant_name", "merchant_category", "region", "commission_rate",
    "hour", "day_of_week", "month", "is_weekend", "day_period", "is_working_hours",
    "transaction_rank", "total_transactions_user",
    "montant_cumule_7j", "is_active_user", "jours_depuis_achat_precedent",
    "ecart_panier_moyen_pct", "is_suspicious"
  )

  /** Sortie transactions_enrichies : uniquement les colonnes documentées dans le sujet. */
  def toEnrichedOutput(full: DataFrame): DataFrame =
    full.select(enrichedColumns.map(col): _*)

  /** Sortie transactions_suspectes : lignes is_suspicious = 1 triées par amount décroissant. */
  def suspiciousTransactions(full: DataFrame): DataFrame =
    full.filter(col("is_suspicious") === 1)
      .select(
        col("transaction_id"), col("user_id"), col("merchant_id"), col("transaction_date"), col("amount"),
        round(col("panier_moyen_user"), 2).as("panier_moyen_user"),
        col("ecart_panier_moyen_pct"), col("day_period"), col("delai_precedente_minutes"),
        col("payment_method"),
        col("cond_montant"), col("cond_night"), col("cond_delai"), col("cond_crypto"),
        col("nb_conditions"), col("is_suspicious"))
      .orderBy(col("amount").desc)
}
