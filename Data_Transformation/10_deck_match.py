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
from itertools import islice
import time

# Configuration ultra-optimisée
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'
BATCH_SIZE = 500000  # ÉNORME batch pour insertion massive
PROGRESS_INTERVAL = 100000

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

def create_deck_match_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS deck_match;
            CREATE UNLOGGED TABLE deck_match (
                id SERIAL,
                match_id INT NOT NULL,
                player_id TEXT NOT NULL,
                deck_id TEXT,
                wins SMALLINT DEFAULT 0,
                draws SMALLINT DEFAULT 0,
                losses SMALLINT DEFAULT 0
            );
        """)
        conn.commit()
        print("[OK] Table UNLOGGED 'deck_match' créée.")

def populate_deck_match_ultra_fast(conn):
    start_time = time.time()
    
    print("[INFO] Insertion directe avec SQL pur...")
    print("[INFO] Utilisation du deck_id = player_id + '_' + tournament_id")
    
    with conn.cursor() as cur:
        # MODIFICATION : Création du deck_id par concaténation directe en SQL
        cur.execute("""
            INSERT INTO deck_match (match_id, player_id, deck_id, wins, draws, losses)
            SELECT 
                m.match_id,
                m.player1_id as player_id,
                CONCAT(m.player1_id, '_', m.tournament_id) as deck_id,
                CASE WHEN m.match_winner = m.player1_id THEN 1 ELSE 0 END as wins,
                CASE WHEN m.match_winner IS NULL THEN 1 ELSE 0 END as draws,
                CASE WHEN m.match_winner = m.player2_id THEN 1 ELSE 0 END as losses
            FROM match m
            
            UNION ALL
            
            SELECT 
                m.match_id,
                m.player2_id as player_id,
                CONCAT(m.player2_id, '_', m.tournament_id) as deck_id,
                CASE WHEN m.match_winner = m.player2_id THEN 1 ELSE 0 END as wins,
                CASE WHEN m.match_winner IS NULL THEN 1 ELSE 0 END as draws,
                CASE WHEN m.match_winner = m.player1_id THEN 1 ELSE 0 END as losses
            FROM match m;
        """)
        
        total_inserted = cur.rowcount
        conn.commit()
        
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        
        print(f"[OK] {total_inserted:,} lignes insérées en {elapsed:.1f}s ({rate:,.0f} lignes/sec)")
        
        # Finalisation ultra-rapide
        print("[INFO] Finalisation...")
        cur.execute("ALTER TABLE deck_match SET LOGGED")
        cur.execute("ALTER TABLE deck_match ADD PRIMARY KEY (id)")
        cur.execute("CREATE INDEX idx_deck_match_deck_id ON deck_match(deck_id)")
        cur.execute("CREATE INDEX idx_deck_match_player_id ON deck_match(player_id)")
        cur.execute("CREATE INDEX idx_deck_match_match_id ON deck_match(match_id)")
        conn.commit()

def main():
    try:
        print("[INFO] Démarrage du traitement deck_match ULTRA-RAPIDE...")
        
        conn = get_conn()
        
        create_deck_match_table(conn)
        populate_deck_match_ultra_fast(conn)
        
        conn.close()
        print("[OK] Script ULTRA-RAPIDE terminé avec succès.")
        
    except Exception as e:
        print(f"[ERREUR CRITIQUE] {e}")

if __name__ == '__main__':
    main()
