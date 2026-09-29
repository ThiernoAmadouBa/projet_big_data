package com.ecommerce.utils

import java.io.FileNotFoundException

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileStatus, LocalFileSystem, Path, RawLocalFileSystem}

// =====================================================================
//  Contournement de l'erreur Windows :
//    java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.
//    NativeIO$Windows.access0(java.lang.String, int)'
//
//  Cause : sous Windows, Hadoop appelle du code natif (hadoop.dll / winutils.exe)
//  pour lister un dossier (FileUtil.list -> canRead) et pour changer les permissions
//  (chmod) lors de chaque ecriture. Sans ces binaires, Spark plante des qu'il
//  lit un DOSSIER (products.parquet) ou ecrit un resultat.
//
//  Solution "100 % dans le projet" : un systeme de fichiers local qui fait les memes
//  operations en Java pur (listage via java.io.File, permissions ignorees).
//  Il est active automatiquement sous Windows par SparkSessionBuilder
//  (cle spark.hadoop.fs.file.impl). Aucun effet sous Linux / macOS / cluster.
// =====================================================================

/** Systeme de fichiers "brut" : listage en Java pur, chmod / chown ignores. */
class WindowsSafeRawLocalFileSystem extends RawLocalFileSystem {

  override def listStatus(f: Path): Array[FileStatus] = {
    val local = pathToFile(f)
    if (!local.exists()) throw new FileNotFoundException(s"File $f does not exist")
    if (local.isDirectory) {
      val names: Array[String] = Option(local.list()).getOrElse(Array.empty[String])
      names.map(name => getFileStatus(new Path(f, name)))
    } else {
      Array(getFileStatus(f))
    }
  }

  override def setPermission(p: Path, permission: FsPermission): Unit = ()

  override def setOwner(p: Path, username: String, groupname: String): Unit = ()
}

/**
 * Enveloppe LocalFileSystem (qui gere les fichiers .crc) autour du systeme brut ci-dessus.
 * Le constructeur sans argument est obligatoire : Hadoop instancie la classe par reflexion.
 */
class WindowsSafeLocalFileSystem extends LocalFileSystem(new WindowsSafeRawLocalFileSystem)
