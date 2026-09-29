package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

// =====================================================================
//  Chargement de application.conf avec valeurs par défaut (Partie 7)
// =====================================================================

case class OptimizationConfig(enableCache: Boolean, enableBroadcast: Boolean)

case class InputPaths(transactions: String, users: String, products: String, merchants: String)

case class ValidationConfig(
  minAmount: Double, timestampLength: Int,
  minAge: Int, maxAge: Int, minIncome: Double,
  minPrice: Double, minRating: Double, maxRating: Double,
  minCommission: Double, maxCommission: Double
)

case class AnalyticsConfig(
  behaviorWindowDays: Int, activeMinDays: Int,
  suspiciousAmountPct: Double, suspiciousDelayMinutes: Double,
  suspiciousMinConditions: Int, suspiciousTopN: Int,
  retentionMonth: Int, topProducts: Int
)

case class AppConfig(
  name: String,
  master: String,
  shufflePartitions: Int,
  hadoopHome: String,
  windowsLocalFsFix: Boolean,
  optimization: OptimizationConfig,
  input: InputPaths,
  outputPath: String,
  validation: ValidationConfig,
  analytics: AnalyticsConfig
)

object ConfigLoader {

  /** Charge application.conf (+ propriétés -D et variables d'environnement). */
  def load(): AppConfig = {
    ConfigFactory.invalidateCaches()
    fromConfig(ConfigFactory.load())
  }

  /** Construit l'AppConfig ; toute clé absente prend sa valeur par défaut. */
  def fromConfig(c: Config): AppConfig = AppConfig(
    name              = str(c, "app.name", "EcommerceAnalytics"),
    master            = str(c, "app.spark.master", "local[*]"),
    shufflePartitions = int(c, "app.spark.shuffle.partitions", 8),
    hadoopHome        = str(c, "app.spark.hadoop-home", ""),
    windowsLocalFsFix = bool(c, "app.spark.windows-local-fs-fix", default = true),
    optimization = OptimizationConfig(
      enableCache     = bool(c, "app.optimization.enable-cache", default = true),
      enableBroadcast = bool(c, "app.optimization.enable-broadcast", default = true)
    ),
    input = InputPaths(
      transactions = str(c, "app.data.input.transactions", "src/main/resources/data/transactions.csv"),
      users        = str(c, "app.data.input.users", "src/main/resources/data/users.json"),
      products     = str(c, "app.data.input.products", "src/main/resources/data/products.parquet"),
      merchants    = str(c, "app.data.input.merchants", "src/main/resources/data/merchants.csv")
    ),
    outputPath = str(c, "app.data.output.path", "output/"),
    validation = ValidationConfig(
      minAmount       = dbl(c, "app.validation.transaction.min-amount", 0.0),
      timestampLength = int(c, "app.validation.transaction.timestamp-length", 14),
      minAge          = int(c, "app.validation.user.min-age", 16),
      maxAge          = int(c, "app.validation.user.max-age", 100),
      minIncome       = dbl(c, "app.validation.user.min-income", 0.0),
      minPrice        = dbl(c, "app.validation.product.min-price", 0.0),
      minRating       = dbl(c, "app.validation.product.min-rating", 1.0),
      maxRating       = dbl(c, "app.validation.product.max-rating", 5.0),
      minCommission   = dbl(c, "app.validation.merchant.min-commission", 0.0),
      maxCommission   = dbl(c, "app.validation.merchant.max-commission", 1.0)
    ),
    analytics = AnalyticsConfig(
      behaviorWindowDays      = int(c, "app.analytics.behavior.window-days", 7),
      activeMinDays           = int(c, "app.analytics.behavior.active-min-days", 5),
      suspiciousAmountPct     = dbl(c, "app.analytics.suspicious.amount-excess-pct", 300.0),
      suspiciousDelayMinutes  = dbl(c, "app.analytics.suspicious.delay-minutes", 5.0),
      suspiciousMinConditions = int(c, "app.analytics.suspicious.min-conditions", 2),
      suspiciousTopN          = int(c, "app.analytics.suspicious.top-n", 20),
      retentionMonth          = int(c, "app.analytics.cohort.retention-month", 3),
      topProducts             = int(c, "app.analytics.products.top-n", 10)
    )
  )

  // --- petits utilitaires "valeur par défaut si la clé est absente" ---
  private def str(c: Config, path: String, default: String): String =
    if (c.hasPath(path)) c.getString(path) else default
  private def int(c: Config, path: String, default: Int): Int =
    if (c.hasPath(path)) c.getInt(path) else default
  private def dbl(c: Config, path: String, default: Double): Double =
    if (c.hasPath(path)) c.getDouble(path) else default
  private def bool(c: Config, path: String, default: Boolean): Boolean =
    if (c.hasPath(path)) c.getBoolean(path) else default
}
