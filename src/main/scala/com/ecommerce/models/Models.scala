package com.ecommerce.models

import org.apache.spark.sql.{DataFrame, Dataset}

// =====================================================================
//  Case classes des 4 jeux de données (Membre A)
//  Les noms des champs sont volontairement en snake_case : ils doivent
//  correspondre exactement aux noms de colonnes pour que Spark puisse
//  faire la conversion DataFrame -> Dataset[T] avec .as[T].
//  Les champs pouvant être vides (valeurs manquantes) sont des Option.
// =====================================================================

case class Transaction(
  transaction_id: String,
  user_id: String,
  product_id: String,
  merchant_id: String,
  amount: Option[Double],
  timestamp: String,        // format yyyyMMddHHmmss
  location: String,
  payment_method: String,
  category: String
)

case class User(
  user_id: String,
  age: Option[Int],
  annual_income: Option[Double],
  city: String,
  customer_segment: String,
  preferred_categories: Option[Seq[String]],
  registration_date: String // format yyyyMMdd
)

case class Product(
  product_id: String,
  name: String,
  category: String,
  price: Option[Double],
  merchant_id: String,
  rating: Option[Double],
  stock: Option[Int]
)

case class Merchant(
  merchant_id: String,
  name: String,
  category: String,
  region: String,
  commission_rate: Option[Double],
  establishment_date: String // format yyyyMMdd
)

/** Résultat brut de l'ingestion (avant validation). */
case class IngestedData(
  transactions: Dataset[Transaction],
  users: Dataset[User],
  products: Dataset[Product],
  merchants: Dataset[Merchant]
)

/** Résultat de la validation : lignes valides, lignes rejetées et rapport de qualité. */
case class ValidatedData(
  transactions: Dataset[Transaction],
  users: Dataset[User],
  products: Dataset[Product],
  merchants: Dataset[Merchant],
  rejTransactions: DataFrame,
  rejUsers: DataFrame,
  rejProducts: DataFrame,
  rejMerchants: DataFrame,
  qualityReport: DataFrame
)
