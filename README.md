# ClimaCity Paris — Rendu de projet PySpark



Lydie HAVUGIMANA

---

## Compréhension du projet

Le projet ClimaCity Paris consiste à construire, avec PySpark, un pipeline complet
d'analyse de la disponibilité des vélos en libre-service (Vélib') à Paris, croisée
avec des données météorologiques, en couvrant progressivement les grandes familles
d'API de Spark :

| Notebook | Contenu | API Spark |
|----------|---------|-----------|
| `Spark_DIA3_Session_1.ipynb` | Exploration bas niveau : parsing des snapshots Vélib' bruts, transformations paresseuses, agrégations par clé, jointures | RDD |
| `Spark_DIA3_Session_2.ipynb` | Passage à l'API structurée : nettoyage, feature engineering temporel, jointure avec la météo, écriture Parquet partitionnée | DataFrame |
| `Spark_DIA3_Session_3.ipynb` | Requêtes analytiques métier, fonctions de fenêtrage, persistance transactionnelle avec time-travel et upsert | Spark SQL / Delta Lake |
| `Spark_DIA3_Session_4.ipynb` | Ingestion d'un flux simulé de mises à jour, agrégations sur fenêtres glissantes, détection d'alertes avec état, gestion des données tardives | Structured Streaming |
| `Spark_DIA3_Session_5.ipynb` | Clustering des stations par profil d'usage, régression du taux d'occupation, validation croisée, suivi des expériences | MLlib / MLflow |

L'objectif pédagogique est de montrer qu'un même jeu de données (l'historique
Vélib' 2020-2021 + la météo Paris-Montsouris) peut être abordé avec des niveaux
d'abstraction croissants, chacun ayant ses cas d'usage : le RDD pour comprendre
le modèle d'exécution distribué, le DataFrame/SQL pour la productivité analytique,
Delta Lake pour la fiabilité transactionnelle, Structured Streaming pour le temps
réel, et MLlib pour la valorisation prédictive des données consolidées.

### Écart avec l'énoncé d'origine : la période couverte

Les notebooks fournis par l'enseignant faisaient référence à des données
« 2022-2023 ». Le jeu de données réellement disponible dans le projet
(`data/velib/raw/historique_stations.csv`, téléchargé depuis le dépôt
`lovasoa/historique-velib-opendata`) couvre en réalité **novembre 2020 à avril
2021**. Nous avons donc adapté chaque exercice concerné (jours fériés, partitions
Parquet/Delta demandées par année/mois, splits train/test temporels) à la
période réellement couverte, en le signalant explicitement dans les commentaires
de chaque cellule concernée plutôt que de laisser des valeurs de l'énoncé qui
n'auraient renvoyé aucune ligne.

---

## Choix effectués par phase

**Session 1 (RDD).** Le parsing des lignes CSV brutes est fait à la main avec le
module `csv` plutôt qu'un simple `.split(",")`, pour gérer correctement les
champs contenant des virgules (les coordonnées GPS sont entre guillemets).
`reduceByKey` est systématiquement préféré à `groupByKey` pour l'agrégation par
station, afin d'illustrer la combinaison locale avant shuffle.

**Session 2 (DataFrame).** Le nettoyage est écrit comme une chaîne unique de
`.withColumn()` plutôt que des réaffectations successives de variables, pour
que le plan Catalyst optimise l'ensemble d'un coup. Les téléchargements
(stations GBFS, historique Vélib', météo Open-Meteo) vérifient désormais
l'existence du fichier cible avant d'appeler le réseau, pour ne pas retélécharger
inutilement à chaque `Restart & Run All`.

**Session 3 (Spark SQL / Delta).** Les questions métier sont écrites en SQL pur
(`spark.sql(...)`) plutôt qu'en API DataFrame, conformément à l'objectif
pédagogique de la section ; le fenêtrage analytique (`LAG`, `ROW_NUMBER`, moyenne
mobile) est en revanche fait en API fonctionnelle pour comparer les deux styles.
Le `MERGE INTO` Delta est démontré sur un batch de correction simulé, avec
vérification par `time-travel` (comparaison de version) plutôt qu'une simple
relecture.

**Session 4 (Structured Streaming).** Le simulateur de flux (`scripts/simulateur_flux.py`)
rejoue l'historique sous forme de fichiers JSON déposés dans un répertoire
surveillé (`file source`), pour éviter d'avoir à monter une infrastructure Kafka
pour l'exercice. La détection d'alertes de rupture de stock utilise `foreachBatch`
avec un état Python conservé entre micro-batchs plutôt qu'une agrégation fenêtrée
pure, car la logique (« combien de temps une station reste-t-elle vide avant de
se reconstituer ») ne se laisse pas exprimer par une simple fenêtre temporelle.

**Session 5 (MLlib).** Le split train/test est fait **chronologiquement**
(la dernière année disponible sert de test) et non aléatoirement, pour éviter la
fuite d'information propre aux séries temporelles. Le clustering K-Means est
appliqué sur un vecteur de 24 features (le profil horaire moyen de chaque
station) plutôt que sur les variables brutes, ce qui donne des clusters
interprétables (« stations pendulaires », « stations stables »...). Le nombre de
clusters (k=4) est choisi à l'œil sur la courbe du coude (méthode WSSSE), une
approche qualitative jugée suffisante à ce stade du projet.

---

## Transparence sur l'utilisation d'un assistant IA

Conformément à la consigne de l'énoncé, nous indiquons explicitement avoir eu
recours à un assistant IA (**Claude / Claude Code**, Anthropic) au cours de ce
projet, en particulier pour :

- **Déboguer l'environnement d'exécution** (Docker, kernels Jupyter, permissions
  de volumes, épuisement mémoire de la JVM Spark) — ces problèmes n'ont pas
  d'impact sur le contenu académique du rendu mais nous ont coûté un temps
  important ; l'assistant a permis de diagnostiquer chaque incident précisément
  (ex. lecture des logs du conteneur, inspection des process JVM, vérification
  des dates réellement couvertes par le jeu de données) plutôt que par
  tâtonnement.
- **Compléter des cellules d'exercice restées à trous** dans les Sessions 3, 4 et
  5 (fenêtrage SQL avancé, pipeline Structured Streaming complet — lecture du
  flux, agrégations glissantes, détection d'alertes avec `foreachBatch`,
  injection de données tardives — et pipeline MLlib complet — feature
  engineering, K-Means, GBTRegressor, validation croisée, MLflow), après que
  plusieurs d'entre elles se sont révélées reposer sur des données ou des
  variables qui n'existaient nulle part dans le projet (ex. `station_id` et
  `code_arr`, absents du jeu de données réel ; un jeu de données Parquet
  « pré-préparé » jamais généré par aucune cellule).

**Pourquoi nous jugeons ces solutions correctes.** Chaque proposition a été
vérifiée par rapport aux données réelles du projet plutôt qu'acceptée telle
quelle : la plage de dates réellement couverte par le CSV historique a été
vérifiée directement (`head`/`tail` du fichier) avant d'adapter les exemples
d'années/mois ; le schéma JSON attendu par le lecteur de flux Structured
Streaming a été aligné sur ce que produit réellement `scripts/simulateur_flux.py`
plutôt que sur le schéma supposé par l'énoncé d'origine ; les résultats de
`.explain()`, `.printSchema()` et les comptages intermédiaires ont servi à
confirmer que chaque étape produisait bien ce qui était attendu. Chaque cellule
concernée porte un commentaire à la première personne expliquant ce qui a été
changé et pourquoi (ex. *« J'ai remplacé station_id par nom_station : cette
colonne n'existe pas dans notre schéma »*) — ces commentaires ont été rédigés
avec l'assistant mais reflètent des vérifications que nous avons demandées et
relues.

**Ce qui reste de notre propre analyse.** Les choix de conception rapportés dans
la section précédente (pourquoi un split temporel, pourquoi 24 features horaires
pour le clustering, pourquoi `foreachBatch` plutôt qu'une fenêtre pour les
alertes) sont les nôtres ; l'assistant a implémenté ces choix une fois discutés,
il ne les a pas imposés.

---

## Guide d'installation

Environnement Docker pour le projet PySpark "ClimaCity Paris".
Contient JupyterLab, PySpark 3.5, Delta Lake 3.2 et MLflow 2.17.

### Prérequis

| Outil | Version minimale | Installation |
|-------|-----------------|--------------|
| Docker Desktop | 4.25 | https://www.docker.com/products/docker-desktop/ |
| Docker Compose | 2.20 (inclus dans Docker Desktop) | -- |
| RAM disponible | 8 Go (10-12 Go recommandés alloués à Docker) | -- |
| Espace disque | 10 Go libres | -- |

> **Windows** : Docker Desktop doit être configuré pour utiliser le backend WSL 2
> (Paramètres -> General -> "Use the WSL 2 based engine").
> **macOS Apple Silicon** : l'image est compatible arm64/amd64 via Rosetta.

### Installation (première fois)

```bash
# 1. Cloner le dépôt
# Décompresser l'archive du projet puis se placer dans le dossier
cd Spark

# 2. Ajuster les ressources si nécessaire (fichier .env)
#    DOCKER_MEMORY_LIMIT=10g / SPARK_DRIVER_MEMORY=7g pour un laptop 16 Go

# 3. Construire l'image (inclut les JARs Delta Lake, pas de téléchargement
#    supplémentaire nécessaire ensuite)
docker compose build jupyter
# Durée : 5-10 minutes selon la connexion (télécharge ~2 Go)

# 4. Démarrer les services
docker compose up -d
docker compose ps   # attendre le statut "healthy" sur les deux services
```

> Sur macOS/Linux, `make help` liste des raccourcis équivalents (`make start`,
> `make shell`, `make clean-kernels`...).

### Accès aux services

| Service | URL | Identifiant |
|---------|-----|-------------|
| JupyterLab | http://localhost:8888/lab?token=climacity | Token : `climacity` |
| Spark UI | http://localhost:4040 | Aucun (disponible après `getOrCreate()`) |
| MLflow UI | http://localhost:5001 | Aucun |

### Structure des répertoires

```
Cyclo/
├── docker-compose.yml       # Orchestration des services (jupyter + mlflow)
├── Dockerfile               # Image JupyterLab + PySpark + Delta personnalisée
├── requirements.txt         # Paquets Python ajoutés à l'image de base
├── .env                     # Configuration (ports, mémoire, token)
├── Makefile                 # Commandes de gestion (make help)
│
├── conf/
│   ├── spark-defaults.conf  # Configuration Spark globale (Delta, mémoire…)
│   └── start-climacity.sh   # Script de démarrage du conteneur
│
├── notebooks/               # Les 5 notebooks du projet (Sessions 1 à 5)
│
├── scripts/
│   └── simulateur_flux.py   # Simulateur de flux JSON pour Structured Streaming
│
└── data/                    # NON versionné (voir .gitignore) -- régénéré en
                              # exécutant les notebooks dans l'ordre 1 → 5
```

### Ordre d'exécution

Les notebooks forment un pipeline : chaque session lit les sorties de la
précédente. À exécuter **dans l'ordre**, Session 1 à 5, chacune en
`Restart Kernel and Run All`. La Session 4 nécessite en plus de lancer, dans un
terminal séparé à l'intérieur du conteneur, le simulateur de flux :

```bash
python scripts/simulateur_flux.py --output data/output/stream_input --vitesse 3
```

### Résolution des problèmes courants

- **`OutOfMemoryError` / `Py4JNetworkError` / kernel qui reste bloqué sans rien
  afficher** : le plus souvent, plusieurs kernels restent ouverts en même temps
  (chacun avec sa propre JVM Spark), en particulier via VSCode connecté à un
  serveur Jupyter distant. Lancer `make clean-kernels` avant de relancer un
  notebook, et vérifier que Docker Desktop dispose d'au moins 10 Go de mémoire
  allouée (Settings → Resources → Memory).
- **Le simulateur de flux ne produit pas de fichiers** : il doit tourner dans un
  terminal resté ouvert, à l'intérieur du conteneur (`make shell` puis lancer la
  commande ci-dessus).
