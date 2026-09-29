# EcommerceAnalytics — Système d'analyse de données e-commerce distribué

Projet final **Data Engineer – Spark & Scala** — Groupe 3
(Thierno Amadou BA, Cheikh FAYE, Elimane DIENE — voir `EQUIPE.md`).

Pipeline Spark/Scala qui **ingère** 4 sources (CSV, JSON, Parquet), **valide** les données
(en conservant les lignes rejetées avec leur motif), **enrichit** les transactions (jointures, UDF,
fonctions de fenêtrage), calcule des **KPI métier** (marchands, cohortes, RFM, produits) et
écrit tous les résultats en **CSV + Parquet**.

---------------------------------------------------------------------------------------------------

## 1. Architecture

```
 application.conf ──► ConfigLoader ──► AppConfig ─────────────────────────────────────┐
                                                                                       │
 transactions.csv ┐                                                                    ▼
 users.json       ├─► DataIngestion ─► DataValidation ─► DataTransformation ─► Analytics
 products.parquet │   (Dataset[T])     (valides +        (jointures, UDF,      (KPI, cohortes,
 merchants.csv    ┘                     rejets + rapport) fenêtres, suspects)   RFM, produits)
                                              │                  │                  │
                                              └──────────────────┴──────────────────┘
                                                                 ▼
                                    DataFrameWriterUtils  ──►  output/csv/<nom>/  +  output/parquet/<nom>/
```

| Fichier | Rôle | Propriétaire |
|---|---|---|
| `models/Models.scala` | case classes `Transaction`, `User`, `Product`, `Merchant` | Membre A |
| `analytics/DataIngestion.scala` | lecture des 4 sources + gestion d'erreurs | Membre A |
| `analytics/DataValidation.scala` | règles de validation, rejets, rapport de qualité | Membre A |
| `analytics/TimeFeatures.scala` | UDF `extractTimeFeatures` | Membre B |
| `analytics/DataTransformation.scala` | jointures, fenêtres, transactions suspectes | Membre B |
| `analytics/Analytics.scala` | KPI marchands, cohortes, RFM, produits | Membre C |
| `analytics/SparkOptimizations.scala` | cache / persist / unpersist | Membre C |
| `analytics/MainApp.scala` | orchestration du pipeline + arguments | Membre C |
| `utils/*` | `ConfigLoader`, `SparkSessionBuilder`, `DataFrameWriterUtils`, `PipelineTimer` | commun |

---------------------------------------------------------------------------------------------------

## 2. Prérequis

| Outil | Version | Remarque |
|---|---|---|
| **JDK** | **11 ou 17** (21 fonctionne en général) | `java -version` pour vérifier |
| **SBT** | 1.9.x | télécharge lui-même Scala 2.12.18 et Spark 3.5.1 : **rien d'autre à installer pour `sbt run`** |
| **Scala** | 2.12.18 | géré par SBT (installation séparée facultative) |
| **Apache Spark** | 3.5.1 (build « pre-built for Hadoop 3 ») | **uniquement** pour la commande `spark-submit` |
| IntelliJ IDEA | Community suffit | plugin **Scala** installé |

**Windows** : **rien à installer**. Hadoop réclame normalement `winutils.exe` + `hadoop.dll` ; le projet contient un contournement en Java pur
(`utils/WindowsSafeLocalFileSystem.scala`, activé automatiquement sous Windows, clé `app.spark.windows-local-fs-fix = true`).
Vous verrez donc un avertissement `Did not find winutils.exe` au démarrage : **il est sans conséquence**.
*Solution de repli, si vous préférez la méthode classique* : placez `winutils.exe` et `hadoop.dll` (Hadoop 3.3.x) dans `C:\hadoop\bin\`,
définissez `HADOOP_HOME=C:\hadoop` (ou `app.spark.hadoop-home = "C:/hadoop"`) ; ou travaillez sous WSL2 / Linux.

---------------------------------------------------------------------------------------------------

## 3. Ouvrir et lancer le projet dans IntelliJ IDEA

1. `File > Open…` → sélectionnez le dossier **`EcommerceAnalytics`** (celui qui contient `build.sbt`) → *Open as Project* → **Trust Project**.
2. IntelliJ propose d'importer le projet **sbt** : acceptez. Attendez la fin du téléchargement des dépendances (barre de progression en bas à droite, la 1ʳᵉ fois : quelques minutes).
3. `File > Project Structure > Project > SDK` : choisissez un **JDK 11 ou 17**.
4. Ouvrez `src/main/scala/com/ecommerce/analytics/MainApp.scala`, cliquez sur le triangle vert à côté de `object MainApp` → *Run 'MainApp'*.
   Le premier lancement échoue sur JDK 17+ (erreur `IllegalAccessError … sun.nio.ch`) : c'est normal, corrigez ainsi :
   `Run > Edit Configurations… > MainApp > Modify options > Add VM options`, collez :
   ```
   -Xmx2g -Dfile.encoding=UTF-8 --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
   ```
   et vérifiez que **Working directory** = la racine du projet (`…/EcommerceAnalytics`).
   *(Lancer via la fenêtre `sbt shell` d'IntelliJ — `run` — évite ce réglage : il est déjà dans `build.sbt`.)*
5. Pour passer un argument (`ingestion`, `--no-optim`…) : champ **Program arguments** de la configuration.

---------------------------------------------------------------------------------------------------

## 4. Compilation et génération du JAR

Depuis un terminal à la racine du projet :

```bash
sbt compile            # compile
sbt test               # exécute les tests unitaires
sbt assembly           # produit le JAR exécutable :
                       #   target/scala-2.12/ecommerce-analytics.jar
sbt -Dspark.provided=true assembly   # JAR léger (sans Spark) pour un cluster où Spark est installé
```

Le JAR n'embarque pas les fichiers de données (dossier `data/` exclu) : les chemins d'entrée doivent
donc être fournis à l'exécution (voir §6).

---------------------------------------------------------------------------------------------------

## 5. Exécution locale avec SBT

```bash
sbt run                        # pipeline complet (étape "all")
sbt "run ingestion"            # lecture + validation + rapport de qualité + rejets seulement
sbt "run transformation"       # jusqu'aux transactions enrichies et suspectes
sbt "run analytics"            # jusqu'aux KPI, cohortes, RFM, produits
sbt "run all --no-optim"       # sans cache ni broadcast (mesure « avant optimisation »)
sbt "run foo"                  # argument inconnu -> message d'aide, sans exception
```

Les résultats sont écrits dans **`output/`** (voir §8). Chaque exécution affiche dans la console :
bilan lignes lues/valides, rapport de qualité, extraits de chaque résultat et durée de chaque étape.

---------------------------------------------------------------------------------------------------

## 6. Déploiement avec spark-submit

**En local (test du JAR)** — depuis la racine du projet :

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master "local[*]" \
  --driver-memory 2g \
  target/scala-2.12/ecommerce-analytics.jar all
```

**Sur un cluster (YARN)** — les chemins d'entrée/sortie sont surchargés sans modifier le code :

```bash
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master yarn --deploy-mode cluster \
  --num-executors 4 --executor-cores 2 --executor-memory 4g \
  --conf spark.driver.extraJavaOptions="-Dapp.data.input.transactions=hdfs:///ecom/transactions.csv -Dapp.data.input.users=hdfs:///ecom/users.json -Dapp.data.input.products=hdfs:///ecom/products.parquet -Dapp.data.input.merchants=hdfs:///ecom/merchants.csv -Dapp.data.output.path=hdfs:///ecom/output/ -Dapp.spark.shuffle.partitions=200" \
  ecommerce-analytics.jar all
```

Exécution d'une seule étape (Question 6.2) :
`spark-submit --class com.ecommerce.analytics.MainApp app.jar ingestion|transformation|analytics|all`.

---------------------------------------------------------------------------------------------------

## 7. Configuration (`src/main/resources/application.conf`)

Aucun chemin, seuil ou paramètre Spark n'est codé en dur. Toute clé absente reçoit une valeur par défaut (`ConfigLoader`).

| Clé | Défaut | Rôle |
|---|---|---|
| `app.spark.master` | `local[*]` | ignoré si `spark-submit --master` est fourni |
| `app.spark.shuffle.partitions` | 8 | `spark.sql.shuffle.partitions` (Q5.2) |
| `app.spark.hadoop-home` | vide | Windows : dossier contenant `bin\winutils.exe` |
| `app.optimization.enable-cache` | true | cache / persist / unpersist (Q5.1) |
| `app.optimization.enable-broadcast` | true | broadcast des petites tables (Q5.2) |
| `app.data.input.*` | `src/main/resources/data/…` | chemins des 4 sources |
| `app.data.output.path` | `output/` | racine des sorties |
| `app.validation.*` | 16–100 ans, note 1–5, commission 0–1… | seuils de validation |
| `app.analytics.*` | fenêtre 7 j, 5 jours actifs, 300 %, 5 min, top 20, M+3, top 10 | seuils analytiques |

Surcharges sans éditer le fichier : propriété JVM `-Dapp.data.output.path=/tmp/out`,
variables d'environnement `APP_ENABLE_CACHE=false` / `APP_ENABLE_BROADCAST=false`, ou `-Dconfig.file=autre.conf`.

---------------------------------------------------------------------------------------------------

## 8. Sorties produites (`output/`)

| Sortie | Question | Format |
|---|---|---|
| `rapport_qualite_yyyyMMdd.csv` | Q2.4 (+ Q2.5 orphelins) | CSV unique, séparateur `;` |
| `rejets_transactions / rejets_users / rejets_products / rejets_merchants` | Q2.2 | CSV + Parquet |
| `transactions_enrichies` | Q3.2 + Q3.3 (+ Q3.4) | CSV + Parquet |
| `transactions_suspectes` | Q3.4 (bonus) | CSV + Parquet |
| `kpi_marchands` | Q4.1 | CSV + Parquet |
| `cohortes_retention`, `meilleure_cohorte_3m` | Q4.2 | CSV + Parquet |
| `rfm_clients`, `rfm_x_customer_segment` | Q4.3 (bonus) | CSV + Parquet |
| `top10_produits`, `ca_categorie_region`, `ca_paiement_periode` | Q4.4 (bonus) | CSV + Parquet |
| `chronometrage_<mode>_<étape>.csv` | Q5.3 (bonus) | CSV unique |

Arborescence : `output/csv/<nom>/part-*.csv` et `output/parquet/<nom>/part-*.parquet` (mode overwrite,
en-tête CSV). Le CSV des rejets `users` convertit `preferred_categories` en texte via `concat_ws(",", …)`.

---------------------------------------------------------------------------------------------------

## 9. Choix fonctionnels importants

**Règles de validation** (rejet si une règle est violée ; plusieurs motifs séparés par ` | `) :
transactions `amount > 0` et `timestamp` de 14 chiffres ; users âge 16–100 et revenu > 0 ;
products prix > 0 et note 1–5 ; merchants commission entre 0 et 1. Une valeur **manquante** dans un champ contrôlé
est aussi rejetée (`amount_manquant`, `age_manquant`, …). Les valeurs nulles des autres colonnes (ville, mode de paiement…) ne rejettent pas la ligne mais sont comptées dans `nb_valeurs_nulles`.

**Jointures** (transactions ⟕ users ⟕ products ⟕ merchants) : toutes en **LEFT JOIN** afin de conserver « une ligne par transaction valide »,
même quand la référence pointe vers un enregistrement orphelin ou rejeté (les colonnes jointes sont alors nulles).
Les petites tables sont diffusées (`broadcast`).

**Tranches d'âge** : Jeune < 25 ; Adulte 25–44 (le sujet oublie 25 ans : rangé en Adulte) ; Âge Moyen 45–64 ; Senior ≥ 65 ; `Inconnu` si âge absent.

**UDF** : jour et mois en anglais (`Saturday`, `July`) ; `is_working_hours` = 9h ≤ heure ≤ 17h (heures 9 à 17 incluses) ;
`Night` = de 22h à 5h59. Une chaîne nulle/mal formée renvoie une structure nulle, sans faire échouer le job.

**Fenêtres** : `montant_cumule_7j` = somme des montants de l'utilisateur dans l'intervalle [t − 7 jours ; t] ; `is_active_user` = au moins
5 jours calendaires distincts dans les 7 derniers jours (jour courant inclus) ; `jours_depuis_achat_precedent` = différence en jours
calendaires avec le `lag` de la transaction précédente (null pour la première).

**Transactions suspectes** : `panier_moyen_user` = moyenne historique de l'utilisateur ; conditions : montant > 4 × panier moyen
(dépasse de plus de 300 %), période `Night`, délai < 5 min, paiement `CRYPTO` ; suspecte si au moins 2 conditions.

**KPI marchands** : seules les transactions dont le marchand est connu et valide sont comptées ; le pivot par tranche d'âge est réalisé par
agrégation conditionnelle (résultat identique à un pivot, avec 0 si aucune vente).

**Cohortes** : cohorte = mois de la première transaction de l'utilisateur ; `period_index` = mois écoulés depuis ce mois. La « meilleure cohorte à 3 mois »
est celle au plus fort `taux_retention` à `period_index = 3` (égalité : la plus grande cohorte). Attention : une très petite cohorte peut afficher un taux élevé.

**Segmentation RFM (règles justifiées — bonus Q4.3)** — scores 1 à 5 par quintiles (`ntile(5)`, 5 = meilleur ; pour la récence, le client le plus récent obtient 5) :

| Segment | Règle | Justification |
|---|---|---|
| **Champions** | R ≥ 4 **et** F ≥ 4 **et** M ≥ 4 | récents, fréquents et gros dépensiers |
| **Perdus** | R ≤ 2 **et** F ≤ 2 | inactifs depuis longtemps ET peu d'achats : faible valeur, probablement partis |
| **À risque** | R ≤ 2 **et** F ≥ 3 | bons clients passés qui n'achètent plus : à relancer en priorité |
| **Nouveaux** | R ≥ 4 **et** F ≤ 2 | achat récent mais historique court |
| **Clients fidèles** | tous les autres cas | achats réguliers et récence moyenne à bonne |

Les règles sont évaluées dans cet ordre ; chaque utilisateur reçoit donc exactement un segment. Seuls les utilisateurs présents dans `users` sont segmentés.

---------------------------------------------------------------------------------------------------

## 10. Optimisations et mesure du gain (Partie 5)

* `cache()` sur les Datasets bruts/valides et sur les résultats analytiques réutilisés ;
* `persist(StorageLevel.MEMORY_AND_DISK_SER)` sur le DataFrame des transactions enrichies (le plus volumineux) ;
* `unpersist()` explicite en fin de pipeline ;
* `broadcast()` des tables `users`, `products`, `merchants` lors des jointures avec `transactions` ;
* `spark.sql.shuffle.partitions` lu dans `application.conf` (8 pour un poste local, à augmenter sur cluster).

**Comment mesurer (Q5.3)** — chaque exécution écrit `output/chronometrage_<mode>_<étape>.csv` (durées d'`ingestion`,
`transformation`, `analytique`, `ecriture`, `total`) :

```bash
sbt "run all --no-optim"    # -> output/chronometrage_baseline_all.csv   (sans cache ni broadcast)
sbt "run all"               # -> output/chronometrage_optimise_all.csv   (avec optimisations)
```

Reportez les durées ci-dessous (gain % = (sans − avec) / sans × 100). Lancez chaque mode 2 ou 3 fois et prenez la médiane
(la 1ʳᵉ exécution inclut le « chauffage » de la JVM). Les chiffres dépendent de la machine : **ils sont à renseigner après vos exécutions.**

| Étape | Durée sans optimisation (ms) | Durée avec optimisation (ms) | Gain (%) |
|---|---|---|---|
| Ingestion (+ validation) | _à mesurer_ | _à mesurer_ | _à calculer_ |
| Transformation | _à mesurer_ | _à mesurer_ | _à calculer_ |
| Analytique | _à mesurer_ | _à mesurer_ | _à calculer_ |
| Écriture | _à mesurer_ | _à mesurer_ | _à calculer_ |
| **Total** | _à mesurer_ | _à mesurer_ | _à calculer_ |

*Remarque d'interprétation :* Spark est paresseux ; le pipeline force le calcul (`count()`) à la fin de chaque étape pour que les durées soient
attribuées à la bonne étape. Sans cache, les résultats sont recalculés à chaque `count`/`show`/écriture, d'où un coût plus élevé.

---------------------------------------------------------------------------------------------------

## 11. Valeurs de référence pour vérifier vos résultats

Calculées indépendamment avec pandas sur les fichiers fournis (les 138 047 transactions, 12 000 users, 600 marchands) — votre rapport de qualité doit donner des nombres identiques ou très proches
(l'écart éventuel vient de la lecture de valeurs textuelles du type « NA ») :

| dataset | lues | rejetées | taux_rejet |
|---|---|---|---|
| transactions | 138 047 | 1 890 | 1,37 % |
| users | 12 000 | 345 | 2,88 % |
| merchants | 600 | 14 | 2,33 % |
| products | 6 000 | (à lire dans votre rapport) | — |

Transactions dont le `user_id` est orphelin : 400 ; dont le `merchant_id` est orphelin : 250.

---------------------------------------------------------------------------------------------------

## 12. Tests

`sbt test` lance : `TimeFeaturesTest` (UDF, sans Spark), `DataValidationTest` (règles et motifs de rejet, comptage des nulls et des orphelins),
`DataTransformationTest` (tranches d'âge, fenêtres glissantes, `lag`, utilisateur actif).

---------------------------------------------------------------------------------------------------

## 13. Dépannage

| Symptôme | Cause / solution |
|---|---|
| `UnsatisfiedLinkError … NativeIO$Windows.access0` | Windows sans winutils/hadoop.dll → corrigé par `WindowsSafeLocalFileSystem` (vérifiez `app.spark.windows-local-fs-fix = true` et relancez `sbt clean compile`) |
| `Did not find winutils.exe` (WARN) | avertissement inoffensif, voir §2 |
| Accents illisibles (`├®tape`) dans IntelliJ | `Settings > Build, Execution, Deployment > Build Tools > sbt > VM parameters` : ajoutez `-Dfile.encoding=UTF-8`, puis redémarrez le sbt shell (purement cosmétique) |
| `IllegalAccessError … sun.nio.ch.DirectBuffer` | JDK ≥ 17 lancé depuis IntelliJ sans `--add-opens` → voir §3 étape 4 |
| `Could not locate executable …\winutils.exe` / `HADOOP_HOME unset` | Windows → voir §2 |
| `Path does not exist` / `[ERREUR] fichier introuvable` | mauvais *working directory* (doit être la racine du projet) ou chemin erroné dans `application.conf` |
| `java.lang.NoClassDefFoundError: org/apache/spark/...` au lancement | dépendances Spark en `provided` : n'utilisez `-Dspark.provided=true` que pour le JAR de cluster |
| Caractères `Ã©` dans la console | ajoutez `-Dfile.encoding=UTF-8` aux VM options |
| `OutOfMemoryError` | augmentez `-Xmx` (VM options) ou `--driver-memory` |
