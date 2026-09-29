# EQUIPE — Groupe 3

Projet final Spark & Scala — Système d'analyse de données e-commerce.

> ⚠️ Répartition proposée dans l'ordre de la liste du groupe. **Échangez les rôles si vous le souhaitez**, mais
> gardez trois rôles distincts et mettez à jour ce fichier, `CONTRIBUTIONS.md` et vos commits Git en conséquence.
> Les codes étudiants sont à renseigner par vos soins.

| Membre | Nom, prénom | Code étudiant | Rôle |
|---|---|---|---|
| **A** | BA Thierno Amadou | `À COMPLÉTER` | Data Ingestion & Platform Engineer |
| **B** | FAYE Cheikh | `À COMPLÉTER` | Data Transformation Engineer |
| **C** | DIENE Elimane | `À COMPLÉTER` | Analytics & Performance Engineer |

## Questions traitées par membre

### Membre A — BA Thierno Amadou (Data Ingestion & Platform Engineer)
* **Partie 0** : 0.1 (ce fichier), 0.2 (dépôt Git), 0.3 (journal de contribution)
* **Partie 1** : 1.1 (structure SBT, case classes), 1.2 (`build.sbt`), 1.3 (`README.md`)
* **Partie 2** : 2.1 (`DataIngestion`), 2.2 (validation + rejets), 2.3 (gestion d'erreurs), 2.4 (rapport de qualité), 2.5 *(bonus)* (intégrité référentielle)
* **Partie 7** : 7.1 (`application.conf`, `ConfigLoader`)
* Fichiers : `build.sbt`, `project/`, `models/Models.scala`, `DataIngestion.scala`, `DataValidation.scala`, `application.conf`, `utils/ConfigLoader.scala`, `README.md`

### Membre B — FAYE Cheikh (Data Transformation Engineer)
* **Partie 3** : 3.1 (UDF `extractTimeFeatures`), 3.2 (`enrichTransactionData`), 3.3 (fenêtres), 3.4 *(bonus)* (transactions suspectes)
* Fichiers : `TimeFeatures.scala`, `DataTransformation.scala`, tests `TimeFeaturesTest`, `DataTransformationTest`

### Membre C — DIENE Elimane (Analytics & Performance Engineer)
* **Partie 4** : 4.1 (KPI marchands), 4.2 (cohortes), 4.3 *(bonus)* (RFM), 4.4 *(bonus)* (produits et catégories)
* **Partie 5** : 5.1 (cache/persist), 5.2 (broadcast, shuffle partitions), 5.3 *(bonus)* (gain avant/après)
* **Partie 6** : 6.1 (application principale), 6.2 *(bonus)* (exécution par étape)
* Fichiers : `Analytics.scala`, `SparkOptimizations.scala`, `MainApp.scala`, `utils/DataFrameWriterUtils.scala`, `utils/PipelineTimer.scala`, `utils/SparkSessionBuilder.scala`

### Travail collectif
**Partie 8** (tests, qualité, documentation, soutenance) : chaque membre contribue pour la portion du code dont il est propriétaire
(`DataValidationTest` → A ; `TimeFeaturesTest`, `DataTransformationTest` → B ; validation de l'intégration → C).
