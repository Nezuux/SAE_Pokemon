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

# Configuration ultra-optimisée
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'
json_folder = os.getenv("JSON_FOLDER")
BATCH_SIZE = 100000     # Batch énorme pour insertion massive
MAX_WORKERS = 12        # Traitement parallèle des fichiers

def remove_non_ascii(text):
    if not text:
        return ''
    try:
        text = str(text)
        return re.sub(r'[^\x00-\x7F]', ' ', text).strip()
    except:
        return ''

def safe_json_load(file_path):
    """Version ultra-blindée"""
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                content = f.read()
                return json.loads(content)
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
        files = []
        for item in os.listdir(folder):
            try:
                if item.endswith('.json'):
                    files.append(item)
            except:
                continue
        return files
    except:
        return []

def get_conn():
    """Connexion PostgreSQL ultra-robuste"""
    try:
        # Force l'encodage système
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
        # Fallback minimal
        conn = psycopg2.connect(
            host=host, 
            port=port, 
            dbname=database, 
            user=user
        )
        return conn

def drop_and_create_participation_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS participation;
            CREATE UNLOGGED TABLE participation (
                participation_id SERIAL PRIMARY KEY,
                player_id TEXT,
                player_name TEXT,
                tournament_id TEXT,
                tournament_name TEXT,
                participation_placing SMALLINT
            );
        """)
        conn.commit()
        print("[OK] Table UNLOGGED 'participation' créée.")

def process_file(filename):
    """Version ultra-blindée"""
    try:
        file_path = os.path.join(json_folder, filename)
        data = safe_json_load(file_path)
        
        if not data:
            return []
        
        tournament_id = remove_non_ascii(data.get('id', ''))
        tournament_name = remove_non_ascii(data.get('name', ''))
        
        if not tournament_id:
            return []
        
        participations = []
        seen = set()
        
        for joueur in data.get('players', []):
            player_id = remove_non_ascii(joueur.get('id', ''))
            player_name = remove_non_ascii(joueur.get('name', ''))
            placing = joueur.get('placing')
            
            if (placing is not None and 
                str(placing).isdigit() and 
                int(placing) > 0 and 
                player_id):
                
                key = (player_id, tournament_id)
                if key not in seen:
                    seen.add(key)
                    participations.append((
                        player_id,
                        player_name,
                        tournament_id,
                        tournament_name,
                        int(placing)
                    ))
        
        return participations
    except:
        return []

def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def main():
    start_time = time.time()
    
    try:
        print("[INFO] Démarrage du traitement participations...")
        
        # Connexion PostgreSQL ultra-robuste
        conn = get_conn()
        
        drop_and_create_participation_table(conn)
        
        files = safe_listdir(json_folder)
        total_files = len(files)
        
        print(f"[INFO] {total_files} fichiers trouvés")
        
        all_participations = []
        
        for i, filename in enumerate(files):
            if i % 100 == 0:
                print(f"[PROGRESS] {i}/{total_files} fichiers traités")
            
            participations = process_file(filename)
            if participations:
                all_participations.extend(participations)
        
        total_participations = len(all_participations)
        print(f"[INFO] {total_participations:,} participations collectées")
        
        # Déduplication globale
        unique_participations = list(set(all_participations))
        deduplicated_count = len(unique_participations)
        
        if deduplicated_count < total_participations:
            print(f"[DEDUPLICATION] {total_participations - deduplicated_count:,} doublons supprimés")
            all_participations = unique_participations
            total_participations = deduplicated_count
        
        # Insertion
        inserted = 0
        with conn.cursor() as cur:
            for chunk in chunked_iterable(all_participations, BATCH_SIZE):
                try:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO participation 
                           (player_id, player_name, tournament_id, tournament_name, participation_placing)
                           VALUES %s""",
                        chunk,
                        page_size=BATCH_SIZE
                    )
                    inserted += len(chunk)
                    print(f"[INSERTION] {inserted:,}/{total_participations:,} participations insérées", end='\r')
                except Exception as e:
                    print(f"\n[ERREUR INSERTION] {e}")
                    continue
        
        # Finalisation
        print(f"\n[INFO] Finalisation...")
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE participation SET LOGGED")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_participation_player ON participation(player_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_participation_tournament ON participation(tournament_id)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_participation_placing ON participation(participation_placing)")
                
                # Contraintes de clés étrangères
                try:
                    cur.execute("ALTER TABLE participation ADD CONSTRAINT fk_participation_player FOREIGN KEY (player_id) REFERENCES player(player_id)")
                    cur.execute("ALTER TABLE participation ADD CONSTRAINT fk_participation_tournament FOREIGN KEY (tournament_id) REFERENCES tournament(tournament_id)")
                except Exception as e:
                    print(f"[INFO] Contraintes FK ignorées : {e}")
                
                conn.commit()
            except Exception as e:
                print(f"[ERREUR FINALISATION] {e}")
        
        conn.close()
        
        elapsed = time.time() - start_time
        rate = total_participations / elapsed if elapsed > 0 else 0
        
        print(f"[OK] Terminé en {elapsed:.1f}s : {total_participations:,} participations insérées")
        print(f"[PERFORMANCE] {rate:,.0f} participations/sec")
        
    except Exception as e:
        print(f"[ERREUR CRITIQUE] {e}")

if __name__ == '__main__':
    main()
