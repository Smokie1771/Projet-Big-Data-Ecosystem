# Rapport de Projet Big Data Ecosystem

## Pipeline de Surveillance Météorologique — Smart City

---

## 1. Contexte et cas d'usage

Les villes intelligentes s'appuient sur des réseaux de capteurs IoT pour surveiller les conditions environnementales. Les urbanistes, services d'urgence et agences environnementales ont besoin d'une **analyse historique** des tendances climatiques et d'**alertes en temps réel** lors d'événements extrêmes (vagues de chaleur, tempêtes, rafales dangereuses).

Ce projet construit un pipeline Big Data complet qui collecte les relevés de 10 stations réparties dans une ville, traite les données historiques en batch, ingère les relevés live en streaming, et expose une couche entrepôt SQL pour le reporting.

Ce cas d'usage justifie une approche Big Data selon les trois critères classiques :

- **Volume** : 10 stations, toutes les 15 min → 86 400 enregistrements sur 90 jours ; à l'échelle d'une vraie ville (centaines de stations), ce sont des millions de relevés par jour.
- **Vélocité** : les relevés arrivent en continu et nécessitent une ingestion streaming à faible latence.
- **Variété** : mesures numériques (température, pression, vent), catégorielles (direction), coordonnées GPS et indicateurs booléens d'anomalie.

---

## 2. Données : origine et structure

Les données sont **générées synthétiquement** par un script Python simulant un climat de type parisien (variations saisonnières et diurnes réalistes). Environ 2 % des relevés sont injectés comme anomalies (pics de température, chutes de pression, rafales) pour tester la détection. Deux jeux de données sont produits :

- **CSV historique** — 90 jours × 10 stations × 4 relevés/heure = **86 400 enregistrements** (~9 Mo)
- **JSONL streaming** — 500 enregistrements simulant l'arrivée temps réel (1 toutes les 5 s)

Chaque relevé est **semi-structuré** et contient 12 champs :

| Champ             | Type    | Description                              |
|-------------------|---------|------------------------------------------|
| station_id        | String  | Identifiant de la station (ST001–ST010)  |
| station_name      | String  | Nom de la station                        |
| latitude/longitude| Double  | Coordonnées GPS                          |
| timestamp         | String  | Horodatage (yyyy-MM-dd HH:mm:ss)         |
| temperature_c     | Double  | Température en °C                        |
| humidity_pct      | Double  | Humidité relative (10–100 %)             |
| pressure_hpa      | Double  | Pression atmosphérique en hPa            |
| wind_speed_kmh    | Double  | Vitesse du vent en km/h                  |
| wind_direction    | String  | Direction (N, NE, E, SE, S, SO, O, NO)   |
| precipitation_mm  | Double  | Précipitations en mm                     |
| is_anomaly        | Boolean | Indicateur d'anomalie injectée           |

---

## 3. Architecture Big Data

### Mode de traitement : Hybride (Batch + Streaming)

Le pipeline implémente une **architecture hybride de type Lambda**, combinant traitement batch pour l'analyse historique et streaming pour la surveillance opérationnelle en temps réel.

```
[Capteurs IoT]
      |
      v
[Générateur Python] ──CSV──> [HDFS] ──> [Spark Batch] ──> [Hive Warehouse]
      |                                                            |
      └──JSONL──> [Kafka] ──> [Spark Streaming] ──> [Alertes + Parquet]
```

| Composant                             | Rôle                                                                              |
|---------------------------------------|-----------------------------------------------------------------------------------|
| **HDFS** (namenode + datanode)        | Stockage distribué et tolérant aux pannes pour le CSV historique                  |
| **Apache Kafka** (+ ZooKeeper)        | Courtier de messages ; découple les producteurs des consommateurs streaming       |
| **Spark Batch** (PySpark)             | Agrégations jour/mois, classement des stations, détection d'anomalies (score Z)   |
| **Spark Structured Streaming**        | Consomme Kafka, émet des alertes météo, agrège par fenêtre glissante de 5 min     |
| **Apache Hive** (+ PostgreSQL metastore) | Entrepôt SQL ad hoc sur les tables issues du batch                             |
| **Docker Compose**                    | Orchestre les 9 conteneurs de l'infrastructure                                    |

### Flux du pipeline

1. **Génération** : le script Python produit le CSV historique et le JSONL de streaming.
2. **Ingestion batch** : le CSV est téléversé dans HDFS via la CLI Hadoop.
3. **Ingestion streaming** : le producteur Kafka publie chaque relevé dans le topic `weather-raw` (3 partitions).
4. **Traitement batch (Spark)** : lecture du CSV, calcul des agrégations journalières/mensuelles, classement des stations, détection statistique d'anomalies (score Z > 3). Résultats sauvegardés en Parquet et en tables Hive.
5. **Traitement streaming (Spark)** : consommation du topic Kafka, application des règles d'alerte en temps réel (température > 40 °C / < -10 °C, vent > 80 km/h, pression < 980 hPa) avec classification par type et gravité, fenêtres glissantes de 5 min, archivage Parquet.
6. **Analyse SQL (Hive)** : 7 requêtes HiveQL (journées les plus chaudes, tendances mensuelles, anomalies par station, cycle diurne, comparaison inter-stations, variations jour/jour).

---

## 4. Tests et validation

### Tests unitaires — 17 tests, tous réussis

Suite construite avec `unittest` Python, trois catégories :

- **Générateur (12 tests)** : présence des 12 champs, plages valides de température/humidité/vent/précipitations, direction du vent limitée aux 8 valeurs boussole, taux d'anomalies ~2 %, format d'horodatage, comptage exact des enregistrements (960/jour), 10 stations avec IDs uniques.
- **Intégrité des données (2 tests)** : variation saisonnière (été > hiver), variation diurne (14h > 3h), validés sur 500 échantillons chacun.
- **Compatibilité schéma (3 tests)** : sérialisation JSON aller-retour, correspondance exacte avec le `StructType` Spark Streaming, types de données corrects.

### Tests d'intégration — 12 tests, tous réussis

Validation de l'infrastructure Docker en conditions réelles : accessibilité HDFS (lecture/écriture, réplication), Kafka (broker, création de topic, cycle production/consommation), Spark (master, worker, sorties batch), et vérification end-to-end des fichiers générés.

```
Tests unitaires   : 17 tests en 0.317 s  — OK
Tests intégration : 12 tests en 35.3 s   — OK
Total             : 29 tests, 0 échec
```

---

## 5. Points bonus

| Initiative | Description |
|---|---|
| **Architecture hybride** | Pipeline Lambda complet : batch sur HDFS/Spark + streaming sur Kafka/Spark Structured Streaming |
| **Langage Scala** | Job `WeatherAnalysis.scala` réalisant les mêmes agrégations que le job Python, via l'API native Spark |
| **Détection statistique** | Score Z (seuil > 3 σ) par station sur température et vitesse du vent, au lieu d'un simple seuil fixe |
| **Tests complets** | 29 tests automatisés couvrant l'intégrité des données, la compatibilité des schémas et l'infrastructure |
| **Infrastructure conteneurisée** | 9 services (HDFS, Kafka, Spark, Hive, ZooKeeper, PostgreSQL) reproductibles via `docker compose up` |

---

## Structure du projet

```
BigData_project/
├── docker-compose.yml               # Orchestration (9 conteneurs)
├── hadoop/hadoop.env                # Configuration Hadoop/Hive
├── data/
│   ├── generator/generate_weather_data.py
│   └── sample/                      # CSV (86 400 lignes) + JSONL (500 lignes)
├── kafka/producer.py | consumer.py
├── spark/
│   ├── batch_processing.py          # PySpark batch
│   ├── streaming_processing.py      # PySpark streaming
│   └── WeatherAnalysis.scala        # Bonus Scala
├── hive/queries.hql                 # 7 requêtes HiveQL
├── tests/
│   ├── test_pipeline.py             # 17 tests unitaires
│   └── test_integration.py          # 12 tests d'intégration
└── scripts/run_pipeline.sh          # Orchestration complète
```
