package com.ecommerce.analytics

import java.time.{DayOfWeek, LocalDateTime}
import java.time.format.TextStyle
import java.util.Locale

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Try

/** Structure renvoyée par l'UDF : elle devient une colonne "struct" que l'on éclate ensuite. */
case class TimeFeatures(
  hour: Int,
  day_of_week: String,
  month: String,
  is_weekend: Int,
  day_period: String,
  is_working_hours: Int
)

/**
 * UDF extractTimeFeatures (Membre B - Question 3.1).
 * La logique est dans `compute`, une fonction Scala pure : elle se teste sans Spark.
 */
object TimeFeatures {

  /**
   * Analyse une chaîne yyyyMMddHHmmss. Renvoie None (=> struct null) si la chaîne est
   * null, vide, de mauvaise longueur, non numérique ou correspond à une date impossible :
   * le job ne plante donc jamais.
   */
  def compute(ts: String): Option[TimeFeatures] = {
    if (ts == null) None
    else {
      val s = ts.trim
      if (s.length != 14 || !s.forall(_.isDigit)) None
      else {
        Try(LocalDateTime.of(
          s.substring(0, 4).toInt, s.substring(4, 6).toInt, s.substring(6, 8).toInt,
          s.substring(8, 10).toInt, s.substring(10, 12).toInt, s.substring(12, 14).toInt
        )).toOption.map { dt =>
          val h   = dt.getHour
          val dow = dt.getDayOfWeek
          TimeFeatures(
            hour             = h,
            day_of_week      = dow.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
            month            = dt.getMonth.getDisplayName(TextStyle.FULL, Locale.ENGLISH),
            is_weekend       = if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) 1 else 0,
            day_period       = dayPeriod(h),
            is_working_hours = if (h >= 9 && h <= 17) 1 else 0
          )
        }
      }
    }
  }

  /** Morning [6h-12h[, Afternoon [12h-18h[, Evening [18h-22h[, Night [22h-6h[. */
  def dayPeriod(hour: Int): String =
    if (hour >= 6 && hour < 12) "Morning"
    else if (hour >= 12 && hour < 18) "Afternoon"
    else if (hour >= 18 && hour < 22) "Evening"
    else "Night"

  /** L'UDF Spark : extractTimeFeatures(col("timestamp")) -> struct. */
  val extractTimeFeatures: UserDefinedFunction = udf((ts: String) => compute(ts))
}
