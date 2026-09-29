package com.ecommerce.analytics

import com.ecommerce.models._
import com.ecommerce.utils.ConfigLoader
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataValidationTest extends AnyFunSuite with Matchers with SparkTestSupport {

  private val v = ConfigLoader.load().validation

  test("transactions : montant <= 0 et horodatage mal formé sont rejetés avec leur raison") {
    val s = spark
    import s.implicits._
    val ds = Seq(
      Transaction("T1", "U1", "P1", "M1", Some(10.0), "20240101120000", "Paris", "CARD", "Books"),
      Transaction("T2", "U1", "P1", "M1", Some(-5.0), "20240101120000", "Paris", "CARD", "Books"),
      Transaction("T3", "U1", "P1", "M1", Some(3.0), "2024010112", "Paris", "CARD", "Books"),
      Transaction("T4", "U1", "P1", "M1", Some(0.0), null, "Paris", "CARD", "Books")
    ).toDS()

    val (valid, rejected) = DataValidation.validateTransactions(ds, v)
    valid.count() shouldBe 1L
    rejected.count() shouldBe 3L

    val reasons = rejected.select("transaction_id", DataValidation.RejectionColumn)
      .as[(String, String)].collect().toMap
    reasons("T2") shouldBe "amount <= 0"
    reasons("T3") shouldBe "timestamp_invalide"
    reasons("T4") shouldBe "amount <= 0 | timestamp_invalide"
  }

  test("users : âge hors intervalle et revenu <= 0, raisons cumulées avec ' | '") {
    val s = spark
    import s.implicits._
    val ds = Seq(
      User("U1", Some(30), Some(30000.0), "Paris", "Standard", Some(Seq("Books")), "20230101"),
      User("U2", Some(12), Some(30000.0), "Paris", "Standard", Some(Seq("Books")), "20230101"),
      User("U3", Some(140), Some(-1500.0), "Paris", "Premium", Some(Seq("Toys")), "20230101")
    ).toDS()

    val (valid, rejected) = DataValidation.validateUsers(ds, v)
    valid.count() shouldBe 1L
    val reasons = rejected.select("user_id", DataValidation.RejectionColumn).as[(String, String)].collect().toMap
    reasons("U2") shouldBe "age_hors_intervalle"
    reasons("U3") shouldBe "age_hors_intervalle | income <= 0"
  }

  test("products et merchants : notes et commissions invalides") {
    val s = spark
    import s.implicits._
    val products = Seq(
      Product("P1", "A", "Books", Some(10.0), "M1", Some(4.5), Some(3)),
      Product("P2", "B", "Books", Some(-1.0), "M1", Some(0.0), Some(3))
    ).toDS()
    val (pValid, pRej) = DataValidation.validateProducts(products, v)
    pValid.count() shouldBe 1L
    pRej.select(DataValidation.RejectionColumn).as[String].head() shouldBe "price <= 0 | rating_hors_intervalle"

    val merchants = Seq(
      Merchant("M1", "Shop", "Books", "Bretagne", Some(0.05), "20220101"),
      Merchant("M2", "Shop2", "Books", "Bretagne", Some(2.0), "20220101"),
      Merchant("M3", "Shop3", "Books", "Bretagne", None, "20220101")
    ).toDS()
    val (mValid, mRej) = DataValidation.validateMerchants(merchants, v)
    mValid.count() shouldBe 1L
    mRej.count() shouldBe 2L
  }

  test("rapport : comptage des valeurs nulles et des références orphelines") {
    val s = spark
    import s.implicits._
    val tx = Seq(
      Transaction("T1", "U1", "P1", "M1", Some(10.0), "20240101120000", null, "CARD", "Books"),
      Transaction("T2", "U9", "P1", "M1", Some(10.0), "20240101120000", "Paris", null, "Books")
    ).toDF()
    DataValidation.countNulls(tx) shouldBe 2L

    val users = Seq(User("U1", Some(30), Some(1.0), "Paris", "Standard", None, "20230101")).toDF()
    DataValidation.countOrphans(tx, users, "user_id") shouldBe 1L
    tx.filter(col("user_id") === "U1").count() shouldBe 1L
  }
}
