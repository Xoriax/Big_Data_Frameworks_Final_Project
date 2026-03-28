## Contexte et objectifs
Dans un contexte international marqué par la multiplication et la complexification des conflits armés, les acteurs publics et privés font face à une incertitude croissante quant aux impacts géopolitiques sur les économies, les marchés financiers et les situations humanitaires. Les guerres, qu’elles soient localisées ou à portée internationale, entraînent des conséquences multidimensionnelles : instabilité économique, perturbation des chaînes d’approvisionnement, flux migratoires, crises humanitaires et fluctuations des marchés.
Par ailleurs, les données relatives aux conflits passés et actuels sont souvent dispersées, hétérogènes et difficiles à exploiter efficacement. Cette fragmentation limite la capacité des décideurs à anticiper les risques, à comprendre les dynamiques géopolitiques et à coordonner des actions adaptées.
Dans ce contexte, la mise en place d’une architecture de données structurée, centralisant et valorisant les données géopolitiques, apparaît comme un levier stratégique pour améliorer la prise de décision.
Ainsi notre étude peut d'adresser à différents corps de métier, comme les gouvernements, les entreprises, les institutions financières, les orgnaisations humanitaires et les associations.

---

## Problématique business :
Comment concevoir et déployer une architecture de données capable de collecter, structurer et exploiter des données géopolitiques afin de :
Anticiper les impacts économiques, financiers et humanitaires des conflits armés
Améliorer la prise de décision des différents acteurs (gouvernements, institutions financières, entreprises, organisations humanitaires)
Faciliter la coordination des actions en s’appuyant à la fois sur l’analyse des conflits passés et le suivi des crises actuelles ?


---

## Architecture

Le projet suit une architecture **Medallion** en 3 couches :

![Architecture](ressources/img/image.png)                                                               API REST Flask

---

## Stack technique

| Composant       | Technologie                        |
|-----------------|------------------------------------|
| Stockage brut   | Hadoop HDFS 3.2.1                  |
| Traitement      | Apache Spark 3.0.0 (PySpark)       |
| Metastore       | Apache Hive 2.3.2                  |
| Format de stockage | Parquet (snappy)                |
| Orchestration   | Docker / Docker Compose            |
| API             | Flask + PyArrow                    |
| Langage         | Python 3                           |

---

## Structure du projet

```
pipeline/
  feeder.py       → Ingestion des sources vers HDFS (zone Raw)
  processor.py    → Transformation et écriture dans Hive (zone Curated)
  datamart.py     → Création des tables analytiques (zone Gold)
source/
  war.csv         → Données sur les conflits armés
  economics.sql   → Données économiques liées aux guerres
api/
  app.py          → API REST Flask (lecture zone Gold)
  Dockerfile      → Image Docker de l'API
ressources/logs/  → Logs d'exécution des pipelines
```

---

## Pipeline de données

### 1. Ingestion — `feeder.py`
Lit `war.csv` (séparateur `;`) et `economics.sql` depuis `/source/`, parse les données et écrit des fichiers Parquet partitionnés par `year/month/day` dans HDFS :
- `/data/raw/war_partitioned`
- `/data/raw/economics_partitioned`

### 2. Transformation — `processor.py`
Lit les Parquet bruts depuis HDFS, nettoie et transforme les colonnes, puis écrit les tables Hive managées :
- `default.war_curated`
- `default.economics_curated`

### 3. Datamart — `datamart.py`
Joint les tables curées, calcule des scores (humanitarian, crisis, investment) et écrit les tables gold dans HDFS + Hive :
- `default.gold_output_humanitarian`
- `default.gold_output_government`
- `default.gold_output_finance`

---

## API REST

L'API expose les données de la zone Gold via des endpoints paginés et sécurisés par token.

**Prérequis — fichier `.env`**

Avant de démarrer les conteneurs, créer un fichier `.env` à la racine du projet avec le contenu suivant :
```
API_TOKEN=<votre-token-secret>
```
Ce fichier n'est pas versionné (présent dans `.gitignore`). Choisissez une valeur secrète et robuste.

**Authentification** — ajouter le header suivant à chaque requête :
```
Authorization: Bearer <votre-token-secret>
```

**Endpoints**

| Méthode | URL | Description |
|---------|-----|-------------|
| GET | `/api/gold` | Liste des endpoints disponibles |
| GET | `/api/gold/humanitarian?page=1` | Données humanitaires (5 par page) |
| GET | `/api/gold/government?page=1` | Données gouvernementales (5 par page) |
| GET | `/api/gold/finance?page=1` | Données financières (5 par page) |

**Exemple de réponse**
```json
{
  "page": 1,
  "page_size": 5,
  "total": 100000,
  "total_pages": 20000,
  "data": [ { "id": 1, "name_of_war": "...", "rank": 1, ... } ]
}
```

---

## Lancer le projet

### Prérequis
- Docker Desktop en cours d'exécution

### 1. Démarrer l'infrastructure
```bash
docker-compose up -d
```

### 2. Ingestion des données brutes
```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --executor-cores 2 --total-executor-cores 4 /opt/pipeline/feeder.py
```

### 3. Transformation (Curated)
```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --deploy-mode client --executor-cores 2 --total-executor-cores 4 /opt/pipeline/processor.py
```

### 4. Création des tables Gold
```bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.sql.warehouse.dir=hdfs://namenode:9000/data/gold --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 --deploy-mode client --executor-cores 2 --total-executor-cores 4 /opt/pipeline/datamart.py
```

### 5. Démarrer l'API
```bash
docker-compose up -d --build gold-api
```
L'API est accessible sur `http://localhost:5000`.

---

## Dataset

Les données sources proviennent de Kaggle :

- **war.csv** — données sur les conflits armés dans le monde (pays, type, dates, secteurs impactés...)
- **economics.sql** — indicateurs économiques liés aux guerres (PIB, chômage, pauvreté, marché noir...)

Lien Kaggle : https://www.kaggle.com/datasets/likithagedipudi/war-economic-and-livelihood-impact-dataset/data

---

## Analyse business : 
La mise en place d’une architecture de données géopolitiques permet de centraliser et croiser des indicateurs économiques, financiers et humanitaires issus de conflits passés et en cours afin de produire des analyses fiables et comparables. Les visualisations montrent qu’il est possible d’identifier rapidement les pays les plus vulnérables (via un score de crise combinant inflation, chômage, PIB et dévaluation), de repérer les secteurs économiques les plus exposés ainsi que l’intensité des marchés parallèles (évaluation du risque d’investissement), et d’évaluer l’ampleur des impacts humanitaires comme la pauvreté ou l’insécurité alimentaire.
Une telle architecture permet ainsi d’anticiper les risques, d’orienter les investissements ou les politiques publiques, d’organiser et prioriser les actions humanitaires. Elle facilite également la coordination entre acteurs en fournissant une vision commune, structurée et actualisée des crises, améliorant ainsi la rapidité et la pertinence des décisions.

