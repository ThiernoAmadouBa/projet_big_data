package com.ecommerce.utils

import java.time.LocalDateTime
import scala.collection.mutable

/**
 * Chronomètre des étapes du pipeline (Q5.3 et Q6.2) :
 * journalise l'heure de début, l'heure de fin et la durée de chaque étape,
 * et cumule les durées si une même étape est exécutée plusieurs fois.
 */
class PipelineTimer {
  private val durations = mutable.LinkedHashMap[String, Long]()

  def time[T](step: String)(block: => T): T = {
    val start = System.currentTimeMillis()
    println(s"[TIMER] >>> Début de l'étape '$step' à ${LocalDateTime.now()}")
    try {
      block
    } finally {
      val elapsed = System.currentTimeMillis() - start
      durations(step) = durations.getOrElse(step, 0L) + elapsed
      println(s"[TIMER] <<< Fin de l'étape '$step' à ${LocalDateTime.now()} - durée : $elapsed ms")
    }
  }

  def summary: Seq[(String, Long)] = durations.toSeq
  def totalMs: Long = durations.values.sum
}
