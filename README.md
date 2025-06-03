# SAE_Pokemon 
## Allaire Mathis, Jouanno Florent, Loret Martin, Pavageau Clément — SD3 Groupe D

![image](https://github.com/user-attachments/assets/24f64275-78b6-4873-83cd-1cc67608d513)

---

## Présentation

**SAE_Pokemon** est un projet d’étude de données centré sur le nouveau jeu de cartes à collectionner Pokémon (TCG Pocket).  
L’objectif principal est de collecter, structurer et analyser des informations issues de tournois en ligne, afin de mieux comprendre les tendances de jeu, les stratégies de decks et le métagame de cette nouvelle scène compétitive.

---

## Fonctionnalités principales

- **Web scraping asynchrone** des résultats de tournois sur le site [play.limitlesstcg.com](https://play.limitlesstcg.com)
- Extraction structurée des informations suivantes :
  - Détails des tournois (nom, date, organisateur, format, nombre de joueurs…)
  - Liste des joueurs et leurs classements
  - Decklists complètes pour chaque joueur (cartes jouées, quantités…)
  - Résultats de matchs et pairings
- **Sauvegarde des données** au format JSON
- **Utilisation de PostgreSQL** pour stocker et analyser les données
- Nettoyage automatique des noms de fichiers problématiques (ex. : noms réservés comme `nul` sur Windows)

---

## Structure du projet

- `DataCollection.py` : script principal d’extraction et de structuration des données
- `output/` : fichiers JSON générés pour chaque tournoi
- `cache/` : données mises en cache pour limiter les appels redondants
- `requirements.txt` : dépendances Python nécessaires à l’exécution

---

## Installation

1. **Cloner le dépôt**

```bash
git clone <URL_DU_DEPOT>
cd SAE_Pokemon

## Installer les dépendances

pip install -r requirements.txt



## Dépendances principales
Voici les bibliothèques utilisées dans le projet :

- aiohttp — requêtes HTTP asynchrones

- aiofiles — gestion de fichiers asynchrone

- beautifulsoup4 — parsing HTML

- pandas — traitement de données tabulaires

- psycopg2-binary — connexion à une base de données PostgreSQL

- requests — requêtes HTTP synchrones (pour certains cas)

- Bibliothèques Python standard : json, re, os, time, asyncio, concurrent.futures, urllib.parse, dataclasses
```



## Points techniques notables
- Web scraping rapide et robuste : usage combiné de asyncio, aiohttp et aiofiles pour maximiser la performance.

- Compatibilité multi-plateforme : gestion de cas spécifiques à Windows (noms de fichiers interdits).

- Modularité : architecture du code pensée pour être facilement maintenable et évolutive.

- Pré-traitement intelligent : vérification, nettoyage, standardisation des noms de tournois, decks et joueurs.

- Connexion PostgreSQL : intégration d'une base de données relationnelle pour un traitement plus poussé des données collectées.

## Auteurs
Projet réalisé dans le cadre du BUT SD3 à l’IUT de Vannes — Groupe D

Mathis Allaire

Florent Jouanno

Martin Loret

Clément Pavageau
