package com.ecommerce.analytics

import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel

/**
 * Optimisations Spark (Membre C - Partie 5).
 * Toutes les optimisations sont pilotées par application.conf
 * (app.optimization.enable-cache / enable-broadcast) afin de pouvoir comparer avant / après (Q5.3).
 */
object SparkOptimizations {

  /** cache() = persist(MEMORY_AND_DISK) : pour les DataFrame réutilisés et de taille raisonnable. */
  def cacheIfEnabled[T](ds: Dataset[T], enabled: Boolean): Dataset[T] =
    if (enabled) ds.cache() else ds

  /** Pour les gros DataFrame : stockage sérialisé (moins de mémoire) avec débordement sur disque. */
  def persistSerIfEnabled[T](ds: Dataset[T], enabled: Boolean): Dataset[T] =
    if (enabled) ds.persist(StorageLevel.MEMORY_AND_DISK_SER) else ds

  /** Libère explicitement la mémoire dès qu'un DataFrame n'est plus utile. */
  def release(datasets: Dataset[_]*): Unit =
    datasets.foreach(_.unpersist())
}
