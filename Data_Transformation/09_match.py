# -*- coding: utf-8 -*-
import sys
import os

# Force l'encodage UTF-8 dès le début
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

# Configuration ultra-optimisée
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'
json_folder = os.getenv("JSON_FOLDER")
BATCH_SIZE = 500000  # ÉNORME batch pour insertion massive
MAX_WORKERS = min(16, multiprocessing.cpu_count())  # Utilise tous les CPU disponibles

# Compilation regex pour performance
ASCII_PATTERN = re.compile(r'[^\x00-\x7F]')

def remove_non_ascii(text):
    return ASCII_PATTERN.sub(' ', text).strip()

def safe_json_load(file_path):
    """Version ultra-blindée et rapide"""
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                return json.load(f)  # Directement json.load() plus rapide
        except:
            continue
    
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
            text = content.decode('utf-8', errors='replace')
            return json.loads(text)
    except:
        return None

def safe_listdir(folder):
    """Listage ultra-sécurisé"""
    try:
        return [f for f in os.listdir(folder) if f.endswith('.json')]
    except:
        return []

def get_conn():
    """Connexion PostgreSQL ultra-robuste"""
    try:
        os.environ['PGCLIENTENCODING'] = 'UTF8'
        
        conn = psycopg2.connect(
            host=host, 
            port=port, 
            dbname=database, 
            user=user
        )
        conn.set_client_encoding('UTF8')
        return conn
    except UnicodeDecodeError:
        conn = psycopg2.connect(
            host=host, 
            port=port, 
            dbname=database, 
            user=user
        )
        return conn

def drop_and_create_match_table(conn):
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
    """Traitement parallélisé d'un chunk de fichiers"""
    all_matches = []
    
    for filename in filenames:
        try:
            file_path = os.path.join(json_folder, filename)
            data = safe_json_load(file_path)
            
            if not data:
                continue
            
            tournament_id = remove_non_ascii(data.get('id', ''))
            if not tournament_id:
                continue
            
            for match in data.get('matches', []):
                match_results = match.get('match_results', [])
                if len(match_results) != 2:
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
                        winner = None
                    
                    all_matches.append((
                        tournament_id, p1_id, p1_score, 
                        p2_id, p2_score, winner
                    ))
                except (KeyError, ValueError, TypeError):
                    continue
        except:
            continue
    
    return all_matches

def chunked_files(files, chunk_size):
    """Découpe les fichiers en chunks pour traitement parallèle"""
    for i in range(0, len(files), chunk_size):
        yield files[i:i + chunk_size]

def chunked_iterable(iterable, size):
    """Découpage efficace en chunks"""
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
        
        conn = get_conn()
        drop_and_create_match_table(conn)
        
        # Phase 1: Récupération des fichiers
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
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Découpage des fichiers en chunks pour chaque worker
            file_chunks = list(chunked_files(files, files_per_worker))
            
            # Soumission des tâches
            futures = [executor.submit(process_file_chunk, chunk) for chunk in file_chunks]
            
            # Collecte des résultats
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
        
        # Phase 3: Insertion ultra-rapide par méga-chunks
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
                    if chunk_num % 2 == 0:  # Affichage moins fréquent
                        print(f"[INSERTION] {inserted:,}/{total_matches:,} matches insérés", end='\r')
                except Exception as e:
                    print(f"\n[ERREUR INSERTION] {e}")
                    continue
        
        # Phase 4: Finalisation ultra-rapide
        print(f"\n[INFO] Finalisation...")
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE match SET LOGGED")
                cur.execute("ALTER TABLE match ADD PRIMARY KEY (match_id)")
                cur.execute("CREATE INDEX idx_match_tournament ON match(tournament_id)")
                cur.execute("CREATE INDEX idx_match_players ON match(player1_id, player2_id)")
                cur.execute("CREATE INDEX idx_match_winner ON match(match_winner)")
                
                # Contrainte FK avec gestion d'erreur
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
        
        print(f"[OK] ULTRA-RAPIDE terminé en {elapsed:.1f}s : {total_matches:,} matches insérés")
        print(f"[PERFORMANCE] {rate:,.0f} matches/sec | {total_files} fichiers traités")
        
    except Exception as e:
        print(f"[ERREUR CRITIQUE] {e}")

if __name__ == '__main__':
    main()
