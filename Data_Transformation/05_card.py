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
BATCH_SIZE = 100000
MAX_WORKERS = 12

def clean_url(url):
    """Nettoie l'URL en gardant les caractères valides d'URL"""
    if not url:
        return ''
    try:
        # Garde les caractères valides d'URL : lettres, chiffres, /, :, -, _, ., ?, =, &
        return re.sub(r'[^\w\-\./:?=&]', '', str(url)).strip()
    except:
        return ''

def remove_non_ascii(text):
    """Pour les noms et types seulement"""
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

def drop_and_create_card_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS card CASCADE;
            CREATE UNLOGGED TABLE card (
                card_id TEXT PRIMARY KEY,
                card_name TEXT,
                card_type TEXT
            );
        """)
        conn.commit()
        print("[OK] Table UNLOGGED 'card' créée.")

def process_file(filename):
    """Version ultra-blindée avec URL correcte"""
    try:
        file_path = os.path.join(json_folder, filename)
        data = safe_json_load(file_path)
        
        if not data or not isinstance(data, dict) or 'players' not in data:
            return []
        
        cards = set()
        for player in data['players']:
            for card in player.get('decklist', []):
                if (isinstance(card, dict) and 
                    card.get('url') and 
                    card.get('name') and 
                    card.get('type')):
                    
                    # CORRECTION : Utilisation de clean_url pour garder l'URL intacte
                    card_id = clean_url(card['url'])
                    
                    if card_id:  # Vérification que l'URL n'est pas vide
                        cards.add((
                            card_id,
                            remove_non_ascii(card['name']),
                            remove_non_ascii(card['type'])
                        ))
        
        return list(cards)
    except:
        return []

def deduplicate_on_card_id(cards):
    """Conserve le dernier tuple pour chaque card_id"""
    unique = {}
    for c in cards:
        unique[c[0]] = c
    return list(unique.values())

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
        print("[INFO] Démarrage du traitement cartes...")
        
        conn = get_conn()
        drop_and_create_card_table(conn)
        
        files = safe_listdir(json_folder)
        total_files = len(files)
        
        print(f"[INFO] {total_files} fichiers trouvés")
        
        all_cards = set()
        
        for i, filename in enumerate(files):
            if i % 100 == 0:
                print(f"[PROGRESS] {i}/{total_files} fichiers traités")
            
            cards = process_file(filename)
            if cards:
                all_cards.update(cards)
        
        print(f"[INFO] {len(all_cards):,} cartes uniques collectées, début insertion...")
        
        total_cards = len(all_cards)
        inserted = 0
        
        with conn.cursor() as cur:
            for chunk in chunked_iterable(all_cards, BATCH_SIZE):
                chunk = deduplicate_on_card_id(chunk)
                
                try:
                    psycopg2.extras.execute_values(
                        cur,
                        """INSERT INTO card (card_id, card_name, card_type)
                           VALUES %s
                           ON CONFLICT (card_id) DO UPDATE
                           SET card_name = EXCLUDED.card_name,
                               card_type = EXCLUDED.card_type""",
                        chunk,
                        page_size=BATCH_SIZE
                    )
                    inserted += len(chunk)
                    print(f"[INSERTION] {inserted:,}/{total_cards:,} cartes insérées", end='\r')
                except Exception as e:
                    print(f"\n[ERREUR INSERTION] {e}")
                    continue
        
        print(f"\n[INFO] Finalisation de la table...")
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE card SET LOGGED")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_card_name ON card(card_name)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_card_type ON card(card_type)")
                conn.commit()
            except Exception as e:
                print(f"[ERREUR FINALISATION] {e}")
        
        conn.close()
        
        elapsed = time.time() - start_time
        rate = total_cards / elapsed if elapsed > 0 else 0
        
        print(f"[OK] Terminé en {elapsed:.1f}s : {total_cards:,} cartes uniques insérées")
        print(f"[PERFORMANCE] {rate:,.0f} cartes/sec | {total_files:,} fichiers traités")
        
    except Exception as e:
        print(f"[ERREUR CRITIQUE] {e}")

if __name__ == '__main__':
    main()
