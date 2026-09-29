package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import scala.util.control.NonFatal

/** Erreur claire remontée à l'application quand une source ne peut pas être lue. */
class DataIngestionException(message: String, cause: Throwable) extends RuntimeException(message, cause)

/**
 * Lecture des 4 sources (Membre A - Question 2.1) et conversion en Dataset[T].
 * Les chemins viennent exclusivement de application.conf (AppConfig.input).
 */
class DataIngestion(spark: SparkSession, config: AppConfig) {
  import spark.implicits._

  // Schéma explicite pour transactions.csv (imposé par le sujet)
  private val transactionSchema = StructType(Seq(
    StructField("transaction_id", StringType, nullable = true),
    StructField("user_id", StringType, nullable = true),
    StructField("product_id", StringType, nullable = true),
    StructField("merchant_id", StringType, nullable = true),
    StructField("amount", DoubleType, nullable = true),
    StructField("timestamp", StringType, nullable = true),   // gardé en String : on valide le format nous-mêmes
    StructField("location", StringType, nullable = true),
    StructField("payment_method", StringType, nullable = true),
    StructField("category", StringType, nullable = true)
  ))

  /** transactions.csv : schéma défini explicitement. */
  def readTransactions(): Dataset[Transaction] =
    safeRead("transactions", config.input.transactions) {
      spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(transactionSchema)
        .csv(config.input.transactions)
        .as[Transaction]
    }

  /** users.json : une ligne = un objet JSON ; les champs imbriqués éventuels sont aplatis. */
  def readUsers(): Dataset[User] =
    safeRead("users", config.input.users) {
      val flat = flattenStructs(spark.read.json(config.input.users))
      def pick(name: String, tpe: DataType): Column =
        (if (flat.columns.contains(name)) col(name).cast(tpe) else lit(null).cast(tpe)).as(name)

      flat.select(
        pick("user_id", StringType),
        pick("age", IntegerType),
        pick("annual_income", DoubleType),
        pick("city", StringType),
        pick("customer_segment", StringType),
        pick("preferred_categories", ArrayType(StringType)),
        pick("registration_date", StringType)
      ).as[User]
    }

  /** products.parquet : format colonnaire optimisé, le schéma est dans les fichiers. */
  def readProducts(): Dataset[Product] =
    safeRead("products", config.input.products) {
      val raw = spark.read.parquet(config.input.products)
      raw.select(
        col("product_id").cast(StringType).as("product_id"),
        col("name").cast(StringType).as("name"),
        col("category").cast(StringType).as("category"),
        col("price").cast(DoubleType).as("price"),
        col("merchant_id").cast(StringType).as("merchant_id"),
        col("rating").cast(DoubleType).as("rating"),
        col("stock").cast(IntegerType).as("stock")
      ).as[Product]
    }

  /** merchants.csv : Spark infère le schéma ; on force ensuite les types de la case class. */
  def readMerchants(): Dataset[Merchant] =
    safeRead("merchants", config.input.merchants) {
      val raw = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(config.input.merchants)
      raw.select(
        col("merchant_id").cast(StringType).as("merchant_id"),
        col("name").cast(StringType).as("name"),
        col("category").cast(StringType).as("category"),
        col("region").cast(StringType).as("region"),
        col("commission_rate").cast(DoubleType).as("commission_rate"),
        col("establishment_date").cast(StringType).as("establishment_date")
      ).as[Merchant]
    }

  /** Lit les quatre sources. */
  def readAll(): IngestedData =
    IngestedData(readTransactions(), readUsers(), readProducts(), readMerchants())

  // ------------------------------------------------------------------
  //  Gestion d'erreurs (Question 2.3) : try-catch + nombre de lignes lues
  // ------------------------------------------------------------------
  private def safeRead[T](name: String, path: String)(reader: => Dataset[T]): Dataset[T] = {
    try {
      val ds = reader
      val n  = ds.count() // déclenche la vraie lecture => les erreurs éventuelles sont capturées ici
      println(f"[INGESTION] $name%-13s : $n%,d lignes lues depuis $path")
      ds
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        println(s"[ERREUR] '$name' : fichier introuvable ou structure incorrecte ($path)")
        println(s"         Détail : ${e.getMessage.split("\n").headOption.getOrElse("")}")
        throw new DataIngestionException(s"Lecture impossible de '$name' ($path)", e)
      case NonFatal(e) =>
        println(s"[ERREUR] '$name' : erreur inattendue pendant la lecture ($path) : ${e.getMessage}")
        throw new DataIngestionException(s"Erreur de lecture de '$name' ($path)", e)
    }
  }

  /** Aplatit un niveau de structures imbriquées : address.city -> address_city. */
  private def flattenStructs(df: DataFrame): DataFrame = {
    val cols: Seq[Column] = df.schema.fields.toSeq.flatMap { field =>
      field.dataType match {
        case st: StructType =>
          st.fields.toSeq.map(sf => col(s"${field.name}.${sf.name}").as(s"${field.name}_${sf.name}"))
        case _ => Seq(col(field.name))
      }
    }
    df.select(cols: _*)
  }
}
