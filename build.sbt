// =====================================================================
//  build.sbt - Projet EcommerceAnalytics (Spark 3.5.1 + Scala 2.12.18)
// =====================================================================
//  * Spark 3.5.x est publié pour Scala 2.12 et 2.13 ; on choisit 2.12.18,
//    la version la plus répandue avec Spark 3.5 (voir CONTRIBUTIONS.md).
//  * JDK recommandé : 11 ou 17.
//  * `sbt assembly` produit target/scala-2.12/ecommerce-analytics.jar
//  * `sbt -Dspark.provided=true assembly` produit un JAR léger (sans Spark),
//    à utiliser sur un vrai cluster où Spark est déjà installé.
// =====================================================================

ThisBuild / version      := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.ecommerce"

val sparkVersion = "3.5.1"

// "compile" par défaut => `sbt run` et IntelliJ fonctionnent sans réglage particulier.
val sparkScope: String =
  if (sys.props.get("spark.provided").contains("true")) "provided" else "compile"

// Sur JDK 9+, Spark a besoin de ces options d'accès aux modules internes de la JVM.
val jvmOptions: Seq[String] = {
  // UTF-8 pour les accents, locale en/US pour des nombres formates 138,047 (et non 138 047 avec espace insecable)
  val common = Seq("-Dfile.encoding=UTF-8", "-Dsun.stdout.encoding=UTF-8", "-Dsun.stderr.encoding=UTF-8",
    "-Duser.language=en", "-Duser.country=US", "-Xmx2g")
  val isJava8 = sys.props("java.specification.version").startsWith("1.")
  if (isJava8) common
  else common ++ Seq(
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    "-Djdk.reflect.useDirectMethodHandle=false"
  )
}

lazy val root = (project in file("."))
  .settings(
    name := "EcommerceAnalytics",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % sparkScope,
      "org.apache.spark" %% "spark-sql"  % sparkVersion % sparkScope,
      "com.typesafe"      % "config"     % "1.4.3",
      "org.scalatest"    %% "scalatest"  % "3.2.18"     % Test
    ),

    scalacOptions ++= Seq("-deprecation", "-feature", "-encoding", "UTF-8"),

    Compile / mainClass := Some("com.ecommerce.analytics.MainApp"),

    // On lance Spark dans une JVM séparée (nécessaire pour appliquer jvmOptions)
    run / fork  := true,
    Test / fork := true,
    run / javaOptions  ++= jvmOptions,
    Test / javaOptions ++= jvmOptions,
    Test / parallelExecution := false,
    run / connectInput := true,
    // Les logs de Spark sortent sur stderr : sans cette ligne sbt les affiche tous en rouge "[error]"
    run / outputStrategy := Some(StdoutOutput),

    // ---------------- sbt-assembly : JAR exécutable ----------------
    assembly / mainClass       := Some("com.ecommerce.analytics.MainApp"),
    assembly / assemblyJarName := "ecommerce-analytics.jar",
    assembly / test            := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case PathList("data", _*)                 => MergeStrategy.discard // les données ne sont pas embarquées dans le JAR
      case PathList("output", _*)               => MergeStrategy.discard
      case "reference.conf"                     => MergeStrategy.concat
      case "application.conf"                   => MergeStrategy.concat
      case _                                    => MergeStrategy.first
    }
  )
