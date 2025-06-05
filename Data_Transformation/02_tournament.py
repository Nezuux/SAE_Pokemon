# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
import json
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import time

# Configuration de la connexion à la base de données et des paramètres généraux
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'
json_folder = os.getenv("JSON_FOLDER")  # Dossier contenant les fichiers JSON
BATCH_SIZE = 10000                      # Taille des lots pour les insertions
MAX_WORKERS = 8                         # Nombre maximum de threads pour le traitement parallèle

# Regex compilée pour supprimer les caractères non-ASCII (optimisation de performance)
ASCII_PATTERN = re.compile(r'[^\x00-\x7F]')

def remove_non_ascii(text):
    """Supprime les caractères non-ASCII d'une chaîne de texte."""
    return ASCII_PATTERN.sub(' ', text).strip() if isinstance(text, str) else None

def safe_json_load(file_path):
    """
    Tente de charger un fichier JSON avec différents encodages.
    Gère les erreurs Unicode et JSON pour une meilleure robustesse.
    """
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16', 'utf-8-sig']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return json.load(f)
        except (UnicodeDecodeError, UnicodeError):
            continue
        except json.JSONDecodeError as e:
            print(f"[JSON ERREUR] {file_path} avec {encoding}: {e}")
            continue
        except Exception as e:
            print(f"[ERREUR] {file_path} avec {encoding}: {e}")
            continue
    
    # Dernier recours : remplacer les erreurs de décodage
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERREUR FATALE] Impossible de lire {file_path}: {e}")
        return None

def create_tournament_table(conn):
    """
    Crée la table 'tournament' (non journalisée) dans la base de données.
    Utilisation de UNLOGGED pour accélérer les insertions (pas de WAL).
    """
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS tournament CASCADE;
            CREATE UNLOGGED TABLE tournament (
                tournament_id TEXT,
                tournament_name TEXT,
                tournament_date TIMESTAMP,
                tournament_organizer TEXT,
                tournament_format TEXT,
                tournament_nb_player SMALLINT,
                last_extension TEXT
            );
        """)
        conn.commit()
        print("[OK] Table UNLOGGED 'tournament' créée.")

def process_file(filename):
    """
    Traite un fichier JSON et extrait les données du tournoi.
    Nettoie et valide les champs nécessaires avant insertion.
    """
    file_path = os.path.join(json_folder, filename)
    
    data = safe_json_load(file_path)
    if data is None:
        return None
    
    try:
        tournament_id = remove_non_ascii(data.get('id', ''))
        if not tournament_id:
            return None  # Identifiant manquant ou invalide
        
        # Conversion sécurisée du nombre de joueurs
        nb_players = data.get('nb_players', 0)
        try:
            nb_players = int(nb_players) if nb_players else 0
        except (ValueError, TypeError):
            nb_players = 0
        
        # Construction du tuple de données à insérer
        tournament = (
            tournament_id,
            remove_non_ascii(data.get('name', '')),
            data.get('date'),
            remove_non_ascii(data.get('organizer', '')),
            remove_non_ascii(data.get('format', '')),
            nb_players
        )
        
        return tournament

    except Exception as e:
        print(f"[ATTENTION] Données invalides dans {filename} : {e}")
        return None

def chunked_iterable(iterable, size):
    """
    Génère des sous-listes (chunks) d'une taille donnée à partir d'un itérable.
    Utile pour le traitement en lots.
    """
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def update_last_extension_optimized(conn):
    """
    Met à jour le champ 'last_extension' dans la table 'tournament'
    en fonction de la date du tournoi et des dates de sortie des extensions.
    Version optimisée utilisant une requête SQL unique avec des sous-requêtes.
    """
    start_time = time.time()
    
    with conn.cursor() as cur:
        # Récupère la première extension disponible (hors cas particulier 'P-A')
        cur.execute("""
            SELECT extension_code, extension_date_sortie
            FROM extension
            WHERE extension_code <> 'P-A'
            ORDER BY extension_date_sortie ASC
            LIMIT 1
        """)
        first_ext = cur.fetchone()
        
        if not first_ext:
            print("[ATTENTION] Pas d'extensions valides trouvées.")
            return
        
        first_extension_code, first_extension_date = first_ext
        
        # Mise à jour du champ last_extension pour tous les tournois
        cur.execute("""
            UPDATE tournament 
            SET last_extension = CASE
                WHEN tournament_date IS NULL OR tournament_date < %s THEN %s
                ELSE COALESCE((
                    SELECT e.extension_code
                    FROM extension e
                    WHERE e.extension_date_sortie <= tournament.tournament_date
                      AND e.extension_code <> 'P-A'
                    ORDER BY e.extension_date_sortie DESC
                    LIMIT 1
                ), %s)
            END
        """, (first_extension_date, first_extension_code, first_extension_code))
        
        updated_rows = cur.rowcount
        conn.commit()
