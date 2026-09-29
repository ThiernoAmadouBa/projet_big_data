# CONTRIBUTIONS — Journal de travail du Groupe 3

> Les sections marquées **À COMPLÉTER** doivent être remplies **honnêtement par vous** (heures réelles, difficultés vécues,
> dates et remarques de relecture). Ne les inventez pas : le jury peut vous interroger dessus en soutenance.

## 1. Tableau récapitulatif : question → responsable → relecteur

Membre A = BA Thierno Amadou · Membre B = FAYE Cheikh · Membre C = DIENE Elimane

| Question | Sujet | Responsable | Relecteur |
|---|---|---|---|
| 0.1 / 0.2 / 0.3 | Équipe, Git, journal | A | B |
| 1.1 | Structure SBT + case classes | A | C |
| 1.2 | `build.sbt` | A | C |
| 1.3 | `README.md` | A | C |
| 2.1 | Ingestion multi-format | A | B |
| 2.2 | Validation + rejets | A | B |
| 2.3 | Gestion d'erreurs et résumé | A | B |
| 2.4 | Rapport de qualité | A | B |
| 2.5 *(bonus)* | Intégrité référentielle | A | B |
| 3.1 | UDF `extractTimeFeatures` | B | A |
| 3.2 | `enrichTransactionData` | B | A |
| 3.3 | Fenêtres (cumul 7 j, actif, lag) | B | A |
| 3.4 *(bonus)* | Transactions suspectes | B | A |
| 4.1 | KPI marchands | C | B |
| 4.2 | Cohortes et rétention | C | B |
| 4.3 / 4.4 *(bonus)* | RFM, produits | C | B |
| 5.1 / 5.2 | Cache, persist, broadcast, shuffle | C | A et B |
| 5.3 *(bonus)* | Avant / après optimisation | C | A et B |
| 6.1 / 6.2 | Application principale, étapes | C | A et B (intégration) |
| 7.1 | `application.conf` | A | C |
| Partie 8 | Tests, doc, soutenance | tous | croisée |

## 2. Charge de travail et difficultés (À COMPLÉTER)

| Membre | Heures estimées (honnêtes) | Répartition (codage / tests / doc / relecture) |
|---|---|---|
| A — BA Thierno Amadou | **À COMPLÉTER** h | |
| B — FAYE Cheikh | **À COMPLÉTER** h | |
| C — DIENE Elimane | **À COMPLÉTER** h | |

**Difficultés rencontrées** (exemples de pistes à décrire avec vos mots) :
* A — À COMPLÉTER (ex. : lecture du JSON, schéma explicite des transactions, renommage du fichier `part-*.csv`, configuration Windows/winutils…)
* B — À COMPLÉTER (ex. : `countDistinct` interdit dans une fenêtre, fenêtres glissantes par `rangeBetween`, UDF robuste…)
* C — À COMPLÉTER (ex. : matrice de rétention, mesure du gain avec le paresseux de Spark, ntile sans partition…)

## 3. Journal de relecture croisée (À COMPLÉTER — une entrée datée par module)

Règle : chaque module est relu par un autre membre que son auteur.

| Date | Module relu | Auteur | Relecteur | Remarques formulées |
|---|---|---|---|---|
| `JJ/MM/2026` | `DataIngestion.scala`, `DataValidation.scala` | A | B | |
| `JJ/MM/2026` | `TimeFeatures.scala`, `DataTransformation.scala` | B | A | |
| `JJ/MM/2026` | `Analytics.scala`, `MainApp.scala`, `SparkOptimizations.scala` | C | B (et A) | |
| `JJ/MM/2026` | `build.sbt`, `application.conf`, `README.md` | A | C | |

## 4. Décisions techniques du groupe

**Versions.** Spark **3.5.1** avec Scala **2.12.18** : Spark 3.5 est publié pour Scala 2.12 et 2.13 ; la 2.12 est la plus répandue dans les
distributions Spark/cluster, donc la plus sûre pour un `spark-submit`. JDK 11 ou 17 recommandé. SBT 1.9.9.

**JAR : `assembly` plutôt que `package`.** `package` ne contient que notre code : il faudrait fournir à la main Typesafe Config et toutes
les dépendances au moment du `spark-submit`. `sbt-assembly` produit un JAR autonome (`ecommerce-analytics.jar`) exécutable tel quel.
Les dépendances Spark sont en scope `compile` par défaut (pour que `sbt run` et IntelliJ fonctionnent sans réglage) ; un JAR léger
(`sbt -Dspark.provided=true assembly`) est possible pour un cluster qui fournit déjà Spark.

**Stratégie de jointure.** Base = `transactions` (valides). Trois jointures **LEFT** :
| Table | Type | Justification |
|---|---|---|
| users | LEFT | on conserve toute transaction valide même si l'utilisateur est orphelin ou rejeté (âge/revenu invalides) ; sinon on perdrait du chiffre d'affaires |
| products | LEFT | idem pour un produit inexistant ou rejeté (prix/note invalides) |
| merchants | LEFT | idem pour un marchand inexistant ou rejeté (commission invalide) |

Les trois dimensions (petites : 12 000 / 6 000 / 600 lignes) sont **diffusées** (`broadcast`) et dédoublonnées sur leur clé pour
garantir « une ligne par transaction ». La table `transactions` sert de table de faits ; le sujet demande une ligne par transaction valide.

**Format de sortie.** Chaque résultat en **CSV** (en-tête, `coalesce(1)` pour les petits résultats) **et Parquet**, mode `overwrite`,
via une unique fonction `DataFrameWriterUtils.writeCsvAndParquet`. Le rapport de qualité est un fichier CSV unique (`;`) renommé depuis `part-*.csv`.
`transactions_enrichies` (≈136 000 lignes) n'est pas réduit à un seul fichier CSV.

**Validation.** Motifs de rejet calculés avec `concat_ws(" | ", when(règle1), when(règle2)…)` : une seule passe, plusieurs motifs possibles, chaîne vide = valide.
Une valeur manquante dans un champ contrôlé est rejetée. Seuils lus dans `application.conf`.

**Choix d'interprétation du sujet** (à défendre à l'oral) : 25 ans classé « Adulte » ; `is_working_hours` = 9h à 17h59 ; fenêtre de 7 jours = [t − 7 j ; t] pour le cumul
et 7 jours calendaires (jour courant inclus) pour `is_active_user` ; cohortes définies par les transactions ; RFM : règles détaillées dans le README ; orphelins comptés par rapport
aux jeux de données lus (avant validation).

**Compatibilité Windows.** Sans `winutils.exe`/`hadoop.dll`, Hadoop plante sous Windows (`UnsatisfiedLinkError … NativeIO$Windows.access0`) dès qu'il lit un
dossier (`products.parquet`) ou écrit un résultat. Plutôt que d'imposer l'installation de binaires, nous avons écrit un système de fichiers local en Java pur
(`WindowsSafeLocalFileSystem`, branché via `spark.hadoop.fs.file.impl`, actif uniquement sous Windows). Il liste les dossiers avec `java.io.File` et ignore chmod/chown.

**Optimisations.** `cache()` pour les jeux réutilisés, `persist(MEMORY_AND_DISK_SER)` pour les transactions enrichies, `unpersist()` final, `broadcast()` des petites tables,
`spark.sql.shuffle.partitions` = 8 en local (fichier de config). Le mode « sans optimisation » désactive aussi `spark.sql.autoBroadcastJoinThreshold`.
