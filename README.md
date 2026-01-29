# Projet DuckDB - Intégration Hétérogène

### 1. Prérequis
- Docker & Docker Compose installés
- Système Unix/Linux ou Windows avec WSL
- (Optionnel) Python 3.10 ou plus si utilisaton du script `UseAPI.py`

### 2. Structure du projet
projet_duckdb/
├─ data/                      # Tous les fichiers CSV, JSON, Parquet et images
├─ duckdb/                    # Contient le script SQL
│   └─ integration.sql
├─ docker-compose.yml         # Déploiement Docker de DuckDB
├─ run_demo.sh                # Script pour lancer la démo
└─ README.md                  # Manuel d’installation et déploiement

### 3. Déploiement

1. Cloner le projet
    ```
    git clone <url_du_projet>
    cd projet_duckdb
    ```
2. Placer tous les fichiers de données dans `data/`
3. Donner les droits d'exécution au script :
    ```chmod +x run_demo.sh```
4. Lancer la démo
    ```./run_demo.sh```

Le script effectue : 
- Suppression du conteneur DuckDB existant (si il existe)
- Création et lancement du conteneur DuckDB
- Exécution automatique du script SQL `integration.sql`
- Création de la base de données `/data/mydatabase.duckdb`
- Import des données CSV, JSON, Parquet et Images (BLOB)
- Calculs te affichage des requêtes de test
Le conteneur se termine automatiquement après l'exécution

### 4. Utilisation du script `UseAPI.py`
Le script permet d'interroger une API externe (Open-Meteo), de transformer les données et de les intégrer dans DuckDB : 
Fonctionnement : 
- Appel d'une API REST
- Transformation des données en DataFrame
- Insertion ou analyse via DuckDB
Exécution (hors Docker) : 
```python UseAPI.py```
Ce script nécessité l'installation des dépendances python (duckdb; pandas; requests)

### 5. Contenu du script `integration.sql`
Le script doit contenur : 
1. Initialisation
    - Suppression des tables existantes pour une relance propre
2. Création du schéma
    - Tables : `Categorie`, `Commentaire`, `Competence`, `Membre`, `MotClef`, `Qualification`, `Talent`, `Transaction`, `ImageTransaction`
3. Import des données hétérogènes
    - CSV : `Categorie`, `Competence`, `Membre`, `Transaction`
    - Parquet : `Commentaire`, `Talent`
    - JSON : `MotClef`, `Qualification`
    - Images BLOB : `ImageTransaction`
4. Contrôles d'intégrité
    - Vérifocation de la validité des références avant reconstruction des clés étrangères
5. Requêtes de test
    - Membres ayant le plus dépensé
    - Transactions avce images associées
    - Toutes autre requête d'analyse ou vue nécessaire

### 6. Résultats attendus
Après l'exécution, vosu verrez : 
- Top 10 des bénéficiaires par dépense moyenne
- Top compétences les plus représentées
- Répartition des commentaires par note
- Fréquence des qualifications par mot-clé
- Liste des images de transactions avce métadonnées extraites

### 7. Nettoyage
Pour supprimer la base de données et relancer proprement : 
    ```rm -f data/mydatabase.duckdb```
    ```./run_duckdb.sh```

### 8. Notes
- L'image DuckDB officielle est éphémère, il n'est pas possible d'ouvrir un terminal interactif après exécution
- Pour tester d'autres requêtes, il suffit de les ajouter dans `integration.sql` et de relancer le script
- DuckDB gère nativement CSV, JSON, Parquet et BLOBs, ce qui facilite l'intégration hétérogène

