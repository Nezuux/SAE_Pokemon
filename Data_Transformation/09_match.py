# -*- coding: utf-8 -*-
import sys
import os

# Forcer l'encodage UTF-8 pour les sorties standard et d'erreur
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import psycopg2
import psycopg2.extras
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import time
import multiprocessing

# Configuration des paramètres de connexion et du traitement
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'
json_folder = os.getenv("JSON_FOLDER")  # Dossier contenant les fichiers JSON à traiter
BATCH_SIZE = 500000  # Taille énorme des lots pour insertion massive en base
MAX_WORKERS = min(16, multiprocessing.cpu_count())  # Nombre de threads max selon CPU dispo

# Compilation d'une regex pour retirer les caractères non ASCII (optimisation)
ASCII_PATTERN = re.compile(r'[^\x00-\x7F]')

def remove_non_ascii(text):
    """Supprime tous les caractères non ASCII d'une chaîne"""
    return ASCII_PATTERN.sub(' ', text).strip()

def safe_json_load(file_path):
    """Chargement JSON sécurisé et multi-encodages"""
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    # Tente différents encodages pour charger le fichier
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                return json.load(f)  # Chargement direct JSON
        except:
            continue
    
    # Fallback : lecture en binaire puis décodage en UTF-8 avec remplacement
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
            text = content.decode('utf-8', errors='replace')
            return json.loads(text)
    except:
        return None  # Retourne None si échec total

def safe_listdir(folder):
    """Liste les fichiers JSON dans un dossier, en évitant les erreurs"""
    try:
        return [f for f in os.listdir(folder) if f.endswith('.json')]
    except:
        return []  # Retourne liste vide en cas d'erreur

def get_conn():
    """Connexion robuste à PostgreSQL avec gestion d'encodage"""
    try:
        os.environ['PGCLIENTENCODING'] = 'UTF8'  # Force encodage client UTF-8
        
        conn = psycopg2.connect(
            host=host, 
            port=port, 
            dbname=database, 
            user=user
        )
        conn.set_client_encoding('UTF8')
        return conn
    except UnicodeDecodeError:
        # En cas d'erreur d'encodage, reconnecte sans forcer encodage
        conn = psycopg2.connect(
            host=host, 
            port=port, 
            dbname=database, 
            user=user
        )
        return conn

def drop_and_create_match_table(conn):
    """Supprime et recrée la table 'match' en mode UNLOGGED pour performance"""
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS match;
            CREATE UNLOGGED TABLE match (
                match_id SERIAL,
                tournament_id TEXT,
                player1_id TEXT,
                player1_score SMALLINT,
                player2_id TEXT,
                player2_score SMALLINT,
                match_winner TEXT
            );
        """)
        conn.commit()
        print("[OK] Table UNLOGGED 'match' créée.")

def process_file_chunk(filenames):
    """Traitement en parallèle d'un chunk de fichiers JSON pour extraire les données matches"""
    all_matches = []
    
    for filename in filenames:
        try:
            file_path = os.path.join(json_folder, filename)
            data = safe_json_load(file_path)  # Chargement sécurisé du fichier JSON
            
            if not data:
                continue
            
            tournament_id = remove_non_ascii(data.get('id', ''))  # Nettoyage ID tournoi
            if not tournament_id:
                continue
            
            # Parcours de chaque match dans le fichier JSON
            for match in data.get('matches', []):
                match_results = match.get('match_results', [])
                if len(match_results) != 2:  # On attend exactement deux joueurs
                    continue
                
                try:
                    p1, p2 = match_results
                    p1_id = remove_non_ascii(p1['player_id'])
                    p2_id = remove_non_ascii(p2['player_id'])
                    p1_score = int(p1['score'])
                    p2_score = int(p2['score'])
                    
                    # Détermination rapide du gagnant
                    if p1_score > p2_score:
                        winner = p1_id
                    elif p2_score > p1_score:
                        winner = p2_id
                    else:
                        winner = None  # Égalité
                    
                    # Ajout des données du match à la liste finale
                    all_matches.append((
                        tournament_id, p1_id, p1_score, 
                        p2_id, p2_score, winner
                    ))
                except (KeyError, ValueError, TypeError):
                    continue  # Ignore les entrées mal formées
        except:
            continue  # Ignore les fichiers corrompus
    
    return all_matches

def chunked_files(files, chunk_size):
    """Découpe la liste des fichiers en morceaux (chunks) pour traitement parallèle"""
    for i in range(0, len(files), chunk_size):
        yield files[i:i + chunk_size]

def chunked_iterable(iterable, size):
    """Découpe un itérable en morceaux de taille fixe"""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def main():
    start_time = time.time()
    
    try:
        print("[INFO] Démarrage du traitement matches ULTRA-RAPIDE...")
        
        conn = get_conn()  # Connexion à la base
        drop_and_create_match_table(conn)  # Prépare la table match
        
        # Phase 1: Récupération des fichiers JSON
        files = safe_listdir(json_folder)
        total_files = len(files)
        
        print(f"[INFO] {total_files} fichiers trouvés")
        
        if not files:
            print("[INFO] Aucun fichier à traiter")
            return
        
        # Phase 2: Traitement parallèle massif
        print(f"[INFO] Traitement parallèle avec {MAX_WORKERS} workers...")
        
        all_matches = []
        files_per_worker = max(1, total_files // MAX_WORKERS)
        
        # Utilisation de ThreadPoolExecutor pour paralléliser la lecture et extraction des données
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            file_chunks = list(chunked_files(files, files_per_worker))
            futures = [executor.submit(process_file_chunk, chunk) for chunk in file_chunks]
            
            # Collecte des résultats au fur et à mesure
            processed_chunks = 0
            for future in as_completed(futures):
                matches = future.result()
                if matches:
                    all_matches.extend(matches)
                
                processed_chunks += 1
                print(f"[PARALLEL] {processed_chunks}/{len(file_chunks)} chunks traités | {len(all_matches):,} matches collectés")
        
        total_matches = len(all_matches)
        print(f"[INFO] {total_matches:,} matches collectés, début insertion...")
        
        if not all_matches:
            print("[INFO] Aucun match trouvé")
            return
        
        # Phase 3: Insertion massive en base par lots énormes
        inserted = 0
        with conn.cursor() as cur:
            for chunk_num, chunk in enumerate(chunked_iterable(all_matches, BATCH_SIZE)):
                try:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO match 
                           (tournament_id, player1_id, player1_score, player2_id, player2_score, match_winner)
                           VALUES %s""",
                        chunk,
                        page_size=BATCH_SIZE
                    )
                    
                    inserted += len(chunk)
                    if chunk_num % 2 == 0:  # Affichage régulier mais pas trop fréquent
                        print(f"[INSERTION] {inserted:,}/{total_matches:,} matches insérés", end='\r')
                except Exception as e:
                    print(f"\n[ERREUR INSERTION] {e}")
                    continue
        
        # Phase 4: Finalisation, création d'index et contraintes pour optimiser la table
        print(f"\n[INFO] Finalisation...")
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE match SET LOGGED")  # Rend la table durable après chargement initial
                cur.execute("ALTER TABLE match ADD PRIMARY KEY (match_id)")  # Clé primaire automatique
                cur.execute("CREATE INDEX idx_match_tournament ON match(tournament_id)")  # Index sur tournoi
                cur.execute("CREATE INDEX idx_match_players ON match(player1_id, player2_id)")  # Index sur joueurs
                cur.execute("CREATE INDEX idx_match_winner ON match(match_winner)")  # Index sur gagnant
                
                # Ajout contrainte FK vers tournoi, ignore erreur si la table référencée n'existe pas
                try:
                    cur.execute("ALTER TABLE match ADD CONSTRAINT fk_match_tournament FOREIGN KEY (tournament_id) REFERENCES tournament(tournament_id)")
                except Exception as e:
                    print(f"[INFO] Contrainte FK ignorée : {e}")
                
                conn.commit()
            except Exception as e:
                print(f"[ERREUR FINALISATION] {e}")
        
        conn.close()
        
        elapsed = time.time() - start_time
        rate = total_matches / elapsed if elapsed > 0 else 0
        
        print(f"[OK] ULTRA
