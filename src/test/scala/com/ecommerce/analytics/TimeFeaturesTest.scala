package com.ecommerce.analytics

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests de la logique de l'UDF extractTimeFeatures (sans Spark). */
class TimeFeaturesTest extends AnyFunSuite with Matchers {

  test("samedi 6 juillet 2024, 09h30 : week-end, matin, heures de travail") {
    val tf = TimeFeatures.compute("20240706093000").get
    tf.hour shouldBe 9
    tf.day_of_week shouldBe "Saturday"
    tf.month shouldBe "July"
    tf.is_weekend shouldBe 1
    tf.day_period shouldBe "Morning"
    tf.is_working_hours shouldBe 1
  }

  test("lundi 1er juillet 2024, 23h15 : semaine, nuit, hors heures de travail") {
    val tf = TimeFeatures.compute("20240701231500").get
    tf.day_of_week shouldBe "Monday"
    tf.is_weekend shouldBe 0
    tf.day_period shouldBe "Night"
    tf.is_working_hours shouldBe 0
  }

  test("bornes des périodes de la journée") {
    TimeFeatures.dayPeriod(5)  shouldBe "Night"
    TimeFeatures.dayPeriod(6)  shouldBe "Morning"
    TimeFeatures.dayPeriod(11) shouldBe "Morning"
    TimeFeatures.dayPeriod(12) shouldBe "Afternoon"
    TimeFeatures.dayPeriod(17) shouldBe "Afternoon"
    TimeFeatures.dayPeriod(18) shouldBe "Evening"
    TimeFeatures.dayPeriod(21) shouldBe "Evening"
    TimeFeatures.dayPeriod(22) shouldBe "Night"
  }

  test("bornes des heures de travail (9h à 17h)") {
    TimeFeatures.compute("20240702083000").get.is_working_hours shouldBe 0
    TimeFeatures.compute("20240702090000").get.is_working_hours shouldBe 1
    TimeFeatures.compute("20240702173000").get.is_working_hours shouldBe 1
    TimeFeatures.compute("20240702180000").get.is_working_hours shouldBe 0
  }

  test("entrées invalides : jamais d'exception, résultat None") {
    TimeFeatures.compute(null) shouldBe None
    TimeFeatures.compute("") shouldBe None
    TimeFeatures.compute("2024") shouldBe None
    TimeFeatures.compute("202510201244") shouldBe None       // 12 caractères
    TimeFeatures.compute("2025070908044900") shouldBe None   // 16 caractères
    TimeFeatures.compute("abcdefghijklmn") shouldBe None
    TimeFeatures.compute("20241345000000") shouldBe None     // mois 13
  }
}
