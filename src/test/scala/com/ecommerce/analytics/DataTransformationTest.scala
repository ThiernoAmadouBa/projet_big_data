package com.ecommerce.analytics

import java.sql.Timestamp

import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataTransformationTest extends AnyFunSuite with Matchers with SparkTestSupport {

  private val transfo = new DataTransformation(ConfigLoader.load())

  test("tranches d'âge : Jeune / Adulte / Âge Moyen / Senior / Inconnu") {
    val s = spark
    import s.implicits._
    val df = Seq[(Int, Option[Int])]((1, Some(20)), (2, Some(25)), (3, Some(44)), (4, Some(45)), (5, Some(64)), (6, Some(65)), (7, None))
      .toDF("id", "age")
      .withColumn("g", transfo.ageGroup(col("age")))
    val result = df.select("id", "g").as[(Int, String)].collect().toMap
    result(1) shouldBe "Jeune"
    result(2) shouldBe "Adulte"
    result(3) shouldBe "Adulte"
    result(4) shouldBe "Âge Moyen"
    result(5) shouldBe "Âge Moyen"
    result(6) shouldBe "Senior"
    result(7) shouldBe "Inconnu"
  }

  test("fenêtres : montant cumulé 7 jours, délai depuis l'achat précédent, utilisateur actif") {
    val s = spark
    import s.implicits._
    // Un utilisateur : achats les jours 1, 2, 3, 4, 5 (5 jours distincts) puis le jour 20
    val rows = (1 to 5).map(d => ("T" + d, "U1", Timestamp.valueOf(f"2024-01-$d%02d 10:00:00"), 10.0)) :+
      (("T6", "U1", Timestamp.valueOf("2024-01-20 10:00:00"), 100.0))
    val df = rows.toDF("transaction_id", "user_id", "transaction_date", "amount")

    val out = transfo.addBehaviorFeatures(df)
      .select("transaction_id", "montant_cumule_7j", "is_active_user", "jours_depuis_achat_precedent")
      .as[(String, Double, Int, Option[Int])]
      .collect().map(r => r._1 -> r).toMap

    out("T1")._4 shouldBe None                 // première transaction : pas de précédente
    out("T2")._4 shouldBe Some(1)
    out("T5")._2 shouldBe 50.0                 // 5 x 10 sur la fenêtre de 7 jours
    out("T5")._3 shouldBe 1                    // 5 jours distincts => actif
    out("T4")._3 shouldBe 0                    // seulement 4 jours distincts
    out("T6")._2 shouldBe 100.0                // les anciens achats sont sortis de la fenêtre
    out("T6")._4 shouldBe Some(15)
    out("T6")._3 shouldBe 0
  }
}
