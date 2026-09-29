# GUIDE DE LIVRAISON ET DE SOUTENANCE — Groupe 3

Ce fichier n'est pas exigé par le sujet : c'est votre pense-bête pour terminer proprement.

## 1. Git (Question 0.2) — chaque membre sur son poste

```bash
cd EcommerceAnalytics
git init                                   # UNE seule fois (un membre, puis partage du dossier)
git config user.name "Prénom Nom"          # sur CHAQUE poste, avec SON nom
git config user.email "adresse@mail"
git add .
git commit -m "Ajout de la validation des transactions"
git log --oneline                          # vérifier l'historique
```
Répartition suggérée des commits (chacun ne commite que ses fichiers, cf. `EQUIPE.md`) :
* A : `build.sbt`, `project/`, `models/`, `DataIngestion`, `DataValidation`, `application.conf`, `README.md`, `DataValidationTest`
* B : `TimeFeatures`, `DataTransformation`, `TimeFeaturesTest`, `DataTransformationTest`
* C : `Analytics`, `SparkOptimizations`, `MainApp`, `utils/DataFrameWriterUtils`, `PipelineTimer`, `SparkSessionBuilder`

Le dossier fourni ne contient **pas** de `.git` (il ne peut pas refléter vos vrais commits) : créez-le vous-mêmes.
Faites plusieurs commits par personne, avec des messages clairs (« Ajout de… », « Correction de… »).

## 2. Checklist avant de rendre

- [ ] `sbt compile` et `sbt test` passent sans erreur
- [ ] `sbt "run all"` termine avec « Pipeline terminé avec succès » ; le rapport de qualité montre des taux de rejet **non nuls**
- [ ] `sbt "run all --no-optim"` puis `sbt "run all"` exécutés ; tableau du §10 du `README.md` rempli avec **vos** durées
- [ ] `sbt assembly` → `target/scala-2.12/ecommerce-analytics.jar` ; test : `spark-submit --class com.ecommerce.analytics.MainApp target/scala-2.12/ecommerce-analytics.jar ingestion`
- [ ] `EQUIPE.md` : codes étudiants renseignés
- [ ] `CONTRIBUTIONS.md` : heures, difficultés, relectures **datées** renseignées
- [ ] `git log --oneline` montre les 3 auteurs
- [ ] support de présentation (PowerPoint/PDF) ajouté

## 3. Contenu de l'archive (Question 8.1)

Nom : **`GROUPE_BA_FAYE_DIENE.zip`** (format `GROUPE_<Nom1>_<Nom2>_<Nom3>.zip`) contenant :
code source complet **avec `.git`**, le JAR, `README.md`, `EQUIPE.md`, `CONTRIBUTIONS.md`, `application.conf`,
un échantillon de `output/` (CSV + Parquet), le support de soutenance.
Envoi : **un seul courriel** à `ba2xcar@gmail.com`, envoyé par un membre avec les **deux autres en copie**,
avant le **mardi 06 octobre 2026 à 00h00** (la date du 09 octobre figure aussi dans le sujet : respectez la plus stricte).
Astuce : `output/` et `target/` sont dans `.gitignore` ; ajoutez-les tout de même à l'archive ZIP (pas au dépôt Git).

## 4. Soutenance (Question 8.2) — 5 + 3×5 + 5 minutes

**Présentation collective (5 min)** : contexte, schéma d'architecture (README §1), organisation en 3 rôles, choix structurants
(Spark 3.5.1/Scala 2.12, jointures LEFT + broadcast, sorties CSV+Parquet, configuration externalisée).

**Exposés individuels (5 min chacun)** : présenter son module, une difficulté, et une démo.
* A : lancer `sbt "run ingestion"`, montrer le rapport de qualité et un fichier `rejets_*` avec `rejection_reason` ; expliquer `concat_ws` + `when`, le schéma explicite, l'aplatissement du JSON.
* B : montrer l'UDF sur une chaîne mal formée, expliquer pourquoi `size(collect_set())` remplace `countDistinct` dans une fenêtre, `rangeBetween`, `lag`, les 4 conditions suspectes.
* C : montrer `kpi_marchands`, la matrice de rétention et la meilleure cohorte, le tableau avant/après, les arguments `ingestion|transformation|analytics|all`, la gestion d'erreurs (`finally spark.stop()`).

**Questions du jury** : **chacun peut être interrogé sur le code des autres.** Lisez ensemble tout le projet. Questions probables :
1. Pourquoi LEFT JOIN et pas INNER ? Que deviennent les transactions orphelines ?
2. Différence entre `cache()` et `persist(MEMORY_AND_DISK_SER)` ? Quand appeler `unpersist()` ?
3. Qu'est-ce qu'un shuffle ? Pourquoi `broadcast` sur `merchants` et pas sur `transactions` ?
4. Pourquoi Spark est « paresseux » et en quoi cela complique la mesure des durées ?
5. Différence `rank` / `dense_rank` / `row_number` ; pourquoi `row_number` pour `transaction_rank` ?
6. `rowsBetween` vs `rangeBetween` ; pourquoi `rangeBetween` pour une fenêtre de 7 *jours* ?
7. Comment est calculé `taux_retention` ? Que vaut `period_index = 0` (toujours 100 %) ?
8. Comment les règles RFM sont-elles définies ? Pourquoi `score_r` est-il inversé ?
9. Pourquoi une UDF est-elle plus lente que les fonctions natives ? (pas d'optimisation Catalyst, sérialisation)
10. Pourquoi CSV et Parquet ? (CSV lisible/métier, Parquet colonnaire compressé, schéma conservé)
11. Comment modifier un seuil sans recompiler ? (`application.conf`, `-D…`, variables d'environnement)
12. Que se passe-t-il si `users.json` est absent ? (`DataIngestionException`, message clair, arrêt propre de Spark)
