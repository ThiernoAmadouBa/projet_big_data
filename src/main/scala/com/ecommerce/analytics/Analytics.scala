package com.ecommerce.analytics

import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Analytique business (Membre C - Partie 4).
 * Toutes les méthodes prennent en entrée le DataFrame "transactions_enrichies"
 * (une ligne par transaction valide) produit par DataTransformation.
 */
class Analytics(config: AppConfig) {

  // ------------------------------------------------------------------
  //  Question 4.1 - KPI par marchand
  // ------------------------------------------------------------------
  def kpiMarchands(enriched: DataFrame): DataFrame = {
    // Chiffre d'affaires d'une tranche d'âge (équivalent d'un pivot, avec 0 si aucune vente)
    def caPour(group: String): Column =
      round(sum(when(col("age_group") === group, col("amount")).otherwise(0.0)), 2)

    val perMerchant = enriched
      .filter(col("merchant_name").isNotNull) // transactions dont le marchand est connu et valide
      .groupBy("merchant_id", "merchant_name", "merchant_category", "region")
      .agg(
        round(sum("amount"), 2).as("chiffre_affaires"),
        count(lit(1)).as("nb_transactions"),
        countDistinct("user_id").as("nb_clients_uniques"),
        round(avg("amount"), 2).as("montant_moyen"),
        round(sum(col("amount") * col("commission_rate")), 2).as("commission_totale"),
        caPour("Jeune").as("ca_jeune"),
        caPour("Adulte").as("ca_adulte"),
        caPour("Âge Moyen").as("ca_age_moyen"),
        caPour("Senior").as("ca_senior"),
        round(avg(col("is_suspicious")) * 100, 2).as("taux_suspectes_pct") // bonus Q3.4
      )

    val byCategory = Window.partitionBy("merchant_category").orderBy(col("chiffre_affaires").desc)
    val byRegion   = Window.partitionBy("region").orderBy(col("chiffre_affaires").desc)

    perMerchant
      .withColumn("rang_ca_categorie", rank().over(byCategory))
      .withColumn("rang_ca_region", rank().over(byRegion))
      .select(
        "merchant_id", "merchant_name", "merchant_category", "region",
        "chiffre_affaires", "nb_transactions", "nb_clients_uniques", "montant_moyen",
        "rang_ca_categorie", "rang_ca_region", "commission_totale",
        "ca_jeune", "ca_adulte", "ca_age_moyen", "ca_senior", "taux_suspectes_pct")
      .orderBy(col("chiffre_affaires").desc)
  }

  // ------------------------------------------------------------------
  //  Question 4.2 - Cohortes et rétention
  // ------------------------------------------------------------------

  /** Convertit un index de mois (année*12 + mois) en libellé "yyyy-MM". */
  private def monthLabel(idx: Column): Column = {
    val year  = floor((idx - 1) / 12).cast("int")
    val month = ((idx - 1) % 12 + 1).cast("int")
    concat(year.cast("string"), lit("-"), lpad(month.cast("string"), 2, "0"))
  }

  def cohortRetention(enriched: DataFrame): DataFrame = {
    val tx = enriched
      .filter(col("transaction_date").isNotNull)
      .select(
        col("user_id"), col("amount"),
        (year(col("transaction_date")) * 12 + month(col("transaction_date"))).as("month_idx"))

    // Mois de première transaction de chaque utilisateur = sa cohorte
    val firstMonth = tx.groupBy("user_id").agg(min("month_idx").as("cohort_idx"))

    val cohortSizes = firstMonth
      .withColumn("cohort_month", monthLabel(col("cohort_idx")))
      .groupBy("cohort_month")
      .agg(countDistinct("user_id").as("taille_cohorte"))

    val activity = tx.join(firstMonth, "user_id")
      .withColumn("period_index", (col("month_idx") - col("cohort_idx")).cast("int"))
      .withColumn("cohort_month", monthLabel(col("cohort_idx")))
      .groupBy("cohort_month", "period_index")
      .agg(
        countDistinct("user_id").as("nb_utilisateurs_actifs"),
        sum("amount").as("ca_brut"))

    activity.join(cohortSizes, "cohort_month")
      .withColumn("taux_retention",
        round(col("nb_utilisateurs_actifs") / col("taille_cohorte") * 100, 2))
      .withColumn("chiffre_affaires", round(col("ca_brut"), 2))                                   // bonus
      .withColumn("revenu_moyen_par_utilisateur",
        round(col("ca_brut") / col("nb_utilisateurs_actifs"), 2))                                 // bonus
      .select(
        "cohort_month", "period_index", "taille_cohorte", "nb_utilisateurs_actifs",
        "taux_retention", "chiffre_affaires", "revenu_moyen_par_utilisateur")
      .orderBy("cohort_month", "period_index")
  }

  /** Cohorte ayant le meilleur taux de rétention à M+3 (égalité : cohorte la plus grande). */
  def bestCohort(retention: DataFrame): DataFrame =
    retention
      .filter(col("period_index") === config.analytics.retentionMonth)
      .orderBy(col("taux_retention").desc, col("taille_cohorte").desc)
      .limit(1)
      .select(
        col("cohort_month"),
        col("taille_cohorte"),
        col("nb_utilisateurs_actifs").as("nb_utilisateurs_actifs_m3"),
        col("taux_retention").as("taux_retention_m3"))

  // ------------------------------------------------------------------
  //  Question 4.3 (bonus) - Segmentation RFM
  // ------------------------------------------------------------------
  def rfmClients(enriched: DataFrame): DataFrame = {
    // On ne segmente que les utilisateurs connus dans users (customer_segment renseigné)
    val tx = enriched.filter(col("transaction_date").isNotNull && col("customer_segment").isNotNull)

    val maxDate = tx.agg(max(to_date(col("transaction_date"))).as("_max_date"))

    val perUser = tx.groupBy("user_id").agg(
      first("customer_segment").as("customer_segment"),
      max(to_date(col("transaction_date"))).as("_last_date"),
      count(lit(1)).as("frequence"),
      round(sum("amount"), 2).as("montant"))

    val r = col("score_r")
    val f = col("score_f")
    val m = col("score_m")

    perUser.crossJoin(maxDate)
      .withColumn("recence_jours", datediff(col("_max_date"), col("_last_date")))
      // Quintiles : la récence la plus FAIBLE (client récent) obtient le score 5
      .withColumn("score_r", ntile(5).over(Window.orderBy(col("recence_jours").desc)))
      .withColumn("score_f", ntile(5).over(Window.orderBy(col("frequence").asc)))
      .withColumn("score_m", ntile(5).over(Window.orderBy(col("montant").asc)))
      .withColumn("score_rfm", concat(r.cast("string"), f.cast("string"), m.cast("string")))
      .withColumn("segment_rfm",
        when(r >= 4 && f >= 4 && m >= 4, lit("Champions"))
          .when(r <= 2 && f <= 2, lit("Perdus"))
          .when(r <= 2 && f >= 3, lit("À risque"))
          .when(r >= 4 && f <= 2, lit("Nouveaux"))
          .otherwise(lit("Clients fidèles")))
      .select(
        "user_id", "customer_segment", "recence_jours", "frequence", "montant",
        "score_r", "score_f", "score_m", "score_rfm", "segment_rfm")
  }

  /** Tableau croisé segment RFM x customer_segment déclaré. */
  def rfmCrossCustomerSegment(rfm: DataFrame): DataFrame = {
    val pivoted = rfm
      .groupBy("segment_rfm")
      .pivot("customer_segment")
      .count()
      .na.fill(0L)
    val segmentCols = pivoted.columns.filter(_ != "segment_rfm")
    val total = segmentCols.map(c => col(s"`$c`")).reduce(_ + _)
    pivoted.withColumn("total", total).orderBy("segment_rfm")
  }

  // ------------------------------------------------------------------
  //  Question 4.4 (bonus) - Produits et catégories
  // ------------------------------------------------------------------
  def top10Produits(enriched: DataFrame): DataFrame = {
    val top = enriched
      .filter(col("product_name").isNotNull)
      .groupBy("product_id", "product_name", "rating", "stock")
      .agg(
        first("category").as("category"),
        round(sum("amount"), 2).as("chiffre_affaires"),
        count(lit(1)).as("nb_transactions"))
      .orderBy(col("chiffre_affaires").desc)
      .limit(config.analytics.topProducts)

    top.withColumn("rang", row_number().over(Window.orderBy(col("chiffre_affaires").desc)))
      .select("rang", "product_id", "product_name", "category",
        "chiffre_affaires", "nb_transactions", "rating", "stock")
      .orderBy("rang")
  }

  def caCategorieRegion(enriched: DataFrame): DataFrame =
    enriched
      .filter(col("region").isNotNull)
      .groupBy("region", "category")
      .agg(
        round(sum("amount"), 2).as("chiffre_affaires"),
        count(lit(1)).as("nb_transactions"))
      .withColumn("part_ca_region_pct",
        round(col("chiffre_affaires") / sum("chiffre_affaires").over(Window.partitionBy("region")) * 100, 2))
      .orderBy(col("region"), col("chiffre_affaires").desc)

  def caPaiementPeriode(enriched: DataFrame): DataFrame = {
    val agg = enriched
      .na.fill("INCONNU", Seq("payment_method"))
      .filter(col("day_period").isNotNull)
      .groupBy("payment_method", "day_period")
      .agg(
        round(sum("amount"), 2).as("chiffre_affaires"),
        count(lit(1)).as("nb_transactions"))

    val total = agg.agg(sum("chiffre_affaires")).head().getDouble(0)
    agg.withColumn("part_ca_pct", round(col("chiffre_affaires") / lit(total) * 100, 2))
      .orderBy(col("payment_method"), col("chiffre_affaires").desc)
  }
}
