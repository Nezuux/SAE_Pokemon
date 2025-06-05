# -*- coding: utf-8 -*-
import psycopg2
import psycopg2.extras
import json
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import time

# Configuration ultra-optimisée
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'
json_folder = os.getenv("JSON_FOLDER")
BATCH_SIZE = 10000
MAX_WORKERS = 8

# Compilation regex pour performance
ASCII_PATTERN = re.compile(r'[^\x00-\x7F]')

def remove_non_ascii(text):
    return ASCII_PATTERN.sub(' ', text).strip() if isinstance(text, str) else None

def safe_json_load(file_path):
    """Fonction ultra-robuste pour charger n'importe quel fichier JSON"""
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
    
    # Dernier recours absolu
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERREUR FATALE] Impossible de lire {file_path}: {e}")
        return None

def create_tournament_table(conn):
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
    """Traitement parallélisé d'un fichier JSON - VERSION ULTRA-ROBUSTE"""
    file_path = os.path.join(json_folder, filename)
    
    # Utilisation de la fonction ultra-robuste
    data = safe_json_load(file_path)
    if data is None:
        return None
    
    try:
        # Validation et nettoyage des données
        tournament_id = remove_non_ascii(data.get('id', ''))
        if not tournament_id:
            return None
        
        # Conversion du nombre de joueurs avec validation
        nb_players = data.get('nb_players', 0)
        try:
            nb_players = int(nb_players) if nb_players else 0
        except (ValueError, TypeError):
            nb_players = 0
        
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
    """Découpage efficace en chunks"""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def update_last_extension_optimized(conn):
    """Version optimisée de la mise à jour des extensions"""
    start_time = time.time()
    
    with conn.cursor() as cur:
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
