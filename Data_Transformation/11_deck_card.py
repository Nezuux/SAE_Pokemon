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
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import islice
import time
import multiprocessing
import io
import csv
from collections import defaultdict

# Configuration ULTRA-OPTIMISÉE
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'
json_folder = r"E:\DataCollection\output"#os.getenv("JSON_FOLDER")
BATCH_SIZE = 2000000  # ÉNORME batch pour COPY
MAX_WORKERS = min(32, multiprocessing.cpu_count() * 2)  # Hyperthreading
CHUNK_SIZE = 1000  # Fichiers par worker

def clean_text(text):
    if not text:
        return ''
    # Version ultra-rapide sans regex
    return ''.join(c if ord(c) < 128 else ' ' for c in str(text)).strip()

def safe_json_load(file_path):
    """Version ultra-optimisée avec ujson si disponible"""
    try:
        import ujson as json_lib  # 3x plus rapide que json standard
    except ImportError:
        import json as json_lib
    
    encodings = ['utf-8', 'latin-1', 'cp1252']
    
    for encoding in encodings:
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
                text = content.decode(encoding, errors='replace')
                return json_lib.loads(text)
        except:
            continue
    return None

def get_conn():
    """Connexion PostgreSQL ultra-optimisée (version corrigée)"""
    try:
        os.environ['PGCLIENTENCODING'] = 'UTF8'
        
        conn = psycopg2.connect(
            host=host, 
            port=port, 
            dbname=database, 
            user=user
        )
        conn.set_client_encoding('UTF8')
        
        # Optimisations de session SEULEMENT
        with conn.cursor() as cur:
            cur.execute("SET work_mem = '1GB'")
            cur.execute("SET maintenance_work_mem = '2GB'")
            cur.execute("SET synchronous_commit = off")
            cur.execute("SET checkpoint_completion_target = 0.9")
            cur.execute("SET wal_buffers = '64MB'")
        conn.commit()
        
        return conn
    except Exception:
        return psycopg2.connect(host=host, port=port, dbname=database, user=user)

def create_deck_id(player_id, tournament_id):
    """Création du deck_id par concaténation simple"""
    return f"{player_id}_{tournament_id}"

def create_deck_card_table_optimized(conn):
    """Création de table ultra-optimisée"""
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS deck_card CASCADE;
            CREATE UNLOGGED TABLE deck_card (
                deck_id TEXT,
                card_id TEXT,
                card_name TEXT,
                count INT
            ) WITH (
                fillfactor = 90,
                autovacuum_enabled = false
            );
        """)
        conn.commit()
        print("[OK] Table UNLOGGED deck_card créée (mode vitesse max)")

def process_files_chunk(file_chunk):
    """Traitement ultra-optimisé d'un chunk de fichiers"""
    deck_cards_dict = defaultdict(lambda: defaultdict(int))  # Déduplication native
    
    for filename in file_chunk:
        try:
            file_path = os.path.join(json_folder, filename)
            data = safe_json_load(file_path)
            
            if not data:
                continue
            
            tournament_id = clean_text(data.get('id', ''))
            if not tournament_id:
                continue
            
            for player in data.get('players', []):
                player_id = clean_text(player.get('id', ''))
                decklist = player.get('decklist', [])
                
                if not decklist or not player_id:
                    continue
                
                # Utilisation de la nouvelle fonction create_deck_id
                deck_id = create_deck_id(player_id, tournament_id)
                
                for card in decklist:
                    card_url = card.get('url', '')
                    if not card_url:
                        continue
                    
                    card_id = clean_text(card_url)
                    card_name = clean_text(card.get('name', ''))
                    count = int(card.get('count', 1))
                    
                    key = (deck_id, card_id)
                    if key in deck_cards_dict:
                        deck_cards_dict[key] = max(deck_cards_dict[key], count)
                    else:
                        deck_cards_dict[key] = count
                        
        except Exception:
            continue
    
    # Conversion en format final
    result = []
    for (deck_id, card_id), count in deck_cards_dict.items():
        result.append((deck_id, card_id, '', count))  # card_name vide pour vitesse
    
    return result

def chunked_files(files, chunk_size):
    """Découpage optimisé des fichiers"""
    for i in range(0, len(files), chunk_size):
        yield files[i:i + chunk_size]

def bulk_copy_insert(conn, data):
    """Insertion ultra-rapide avec COPY - La méthode la plus rapide"""
    if not data:
        return 0
    
    # Création d'un buffer CSV en mémoire
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer, delimiter='\t')
    
    for row in data:
        csv_writer.writerow(row)
    
    csv_buffer.seek(0)
    
    # COPY ultra-rapide
    with conn.cursor() as cur:
        cur.copy_from(
            csv_buffer,
            'deck_card',
            columns=('deck_id', 'card_id', 'card_name', 'count'),
            sep='\t'
        )
    
    return len(data)

def create_indexes_without_transaction(conn):
    """Création d'index SANS transaction pour éviter l'erreur CONCURRENTLY"""
    conn.close()
    
    new_conn = psycopg2.connect(
        host=host, 
        port=port, 
        dbname=database, 
        user=user
    )
    new_conn.set_client_encoding('UTF8')
    new_conn.autocommit = True
    
    with new_conn.cursor() as cur:
        try:
            print("[INFO] Création des index sans transaction...")
            
            # PRIMARY KEY d'abord
            new_conn.autocommit = False
            cur.execute("ALTER TABLE deck_card ADD PRIMARY KEY (deck_id, card_id)")
            new_conn.commit()
            
            # Index CONCURRENTLY
            new_conn.autocommit = True
            cur.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_deck_card_card_id ON deck_card(card_id)")
            cur.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_deck_card_deck_id ON deck_card(deck_id)")
            
            # Retour en mode LOGGED
            new_conn.autocommit = False
            cur.execute("ALTER TABLE deck_card SET LOGGED")
            new_conn.commit()
            
            print("[OK] Index créés avec succès")
            
        except Exception as e:
            print(f"[ERREUR INDEX] {e}")
        finally:
            new_conn.close()

def main():
    start_time = time.time()
    
    try:
        print("[INFO] Démarrage ULTRA-RAPIDE deck_card...")
        print("[INFO] deck_id = player_id + '_' + tournament_id")
        
        conn = get_conn()
        create_deck_card_table_optimized(conn)
        
        # Phase 1: Récupération des fichiers
        try:
            files = [f for f in os.listdir(json_folder) if f.endswith('.json')]
        except:
            files = []
        
        total_files = len(files)
        print(f"[INFO] {total_files} fichiers trouvés")
        
        if not files:
            print("[INFO] Aucun fichier à traiter")
            return
        
        # Phase 2: Traitement parallèle MASSIF avec ProcessPoolExecutor
        print(f"[INFO] Traitement parallèle MASSIF avec {MAX_WORKERS} processus...")
        
        all_deck_cards = []
        file_chunks = list(chunked_files(files, CHUNK_SIZE))
        
        # Utilisation de ProcessPoolExecutor pour parallélisme réel
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_files_chunk, chunk) for chunk in file_chunks]
            
            processed_chunks = 0
            for future in as_completed(futures):
                deck_cards = future.result()
                if deck_cards:
                    all_deck_cards.extend(deck_cards)
                
                processed_chunks += 1
                if processed_chunks % 5 == 0:
                    print(f"[PARALLEL] {processed_chunks}/{len(file_chunks)} chunks | {len(all_deck_cards):,} associations")
        
        total_associations = len(all_deck_cards)
        print(f"[INFO] {total_associations:,} associations collectées")
        
        if not all_deck_cards:
            print("[INFO] Aucune association trouvée")
            return
        
        # Phase 3: Insertion ULTRA-RAPIDE avec COPY
        print("[INFO] Insertion ULTRA-RAPIDE avec COPY...")
        
        # Découpage en méga-chunks pour COPY
        inserted = 0
        for i in range(0, len(all_deck_cards), BATCH_SIZE):
            chunk = all_deck_cards[i:i + BATCH_SIZE]
            try:
                inserted_count = bulk_copy_insert(conn, chunk)
                inserted += inserted_count
                print(f"[COPY] {inserted:,}/{total_associations:,} associations insérées", end='\r')
            except Exception as e:
                print(f"\n[ERREUR COPY] {e}")
                continue
        
        conn.commit()
        print(f"\n[INFO] Insertion terminée")
        
        # Phase 4: Finalisation SANS transaction pour les index CONCURRENTLY
        create_indexes_without_transaction(conn)
        
        elapsed = time.time() - start_time
        rate = total_associations / elapsed if elapsed > 0 else 0
        
        print(f"[OK] ULTRA-RAPIDE terminé en {elapsed:.1f}s : {total_associations:,} associations")
        print(f"[PERFORMANCE] {rate:,.0f} associations/sec | {total_files} fichiers")

    except Exception as e:
        print(f"[ERREUR CRITIQUE] {e}")

if __name__ == '__main__':
    main()
