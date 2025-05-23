# SAE_Pokemon

## Présentation

**SAE_Pokemon** est un projet d’étude de données centré sur le nouveau jeu de cartes à collectionner Pokémon (TCG Pocket).  
L’objectif principal est de collecter et d’analyser des informations issues de tournois en ligne, afin de mieux comprendre les tendances de jeu, les stratégies de decks et le métagame de cette nouvelle scène compétitive.

---

## Fonctionnalités principales

- **Web scraping asynchrone** des résultats de tournois sur le site [play.limitlesstcg.com](https://play.limitlesstcg.com).
- Extraction structurée des informations suivantes :
  - Détails des tournois (nom, date, organisateur, format, nombre de joueurs…)
  - Liste des joueurs et de leurs classements
  - Decklists complètes pour chaque joueur (cartes jouées, quantités…)
  - Résultats de matchs et pairings
- **Stockage des données extraites** au format JSON pour faciliter l’analyse et la réutilisation.

---

## Structure du projet

- `DataCollection.py` : script principal d’extraction et de structuration des données.
- `output/` : dossier où sont stockés les fichiers JSON générés pour chaque tournoi.
- `cache/` : dossier de cache pour accélérer les extractions et limiter les requêtes web.
- `requirements.txt` : liste des dépendances Python nécessaires.

---

## Installation

1. **Cloner le dépôt**
git clone <URL_DU_DEPOT>
cd SAE_Pokemon

text

2. **Installer les dépendances**
pip install -r requirements.txt

text
**Principaux modules utilisés :**
- `aiohttp`
- `aiofile`
- `aiofiles`
- `beautifulsoup4`

---

## Utilisation

Lance simplement le script principal pour démarrer la collecte des données :
python DataCollection.py

text
Les données extraites seront automatiquement enregistrées dans le dossier `output/` au format JSON.

---

## Points techniques notables

- **Asynchrone et performant** : le scraping utilise `asyncio`, `aiohttp` et `aiofile` pour maximiser la rapidité et la robustesse.
- **Gestion des cas particuliers Windows** : par exemple, si un joueur s’appelle `nul` (nom réservé sous Windows), il sera automatiquement renommé en `joueur_nul` dans les chemins de fichiers.
- **Modularité** : le code est organisé en fonctions spécialisées pour faciliter la maintenance et l’évolution.
