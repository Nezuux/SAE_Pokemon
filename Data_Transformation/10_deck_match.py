# -*- coding: utf-8 -*-
import sys
import os

# Force l'encodage UTF-8 dès le début pour éviter les problèmes d'encodage dans les sorties standard et d'erreur
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import psycopg2
import psycopg2.extras
from itertools import islice
import time

# Configuration de la connexion à la base PostgreSQL
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'

# Taille des batchs pour insertion massive — très grande pour optimiser les performances
BATCH_SIZE = 500000  
PROGRESS_INTERVAL = 100000  # Intervalle pour afficher la progression (non utilisé dans ce script)

def get_conn():
    """Établit une connexion robuste à PostgreSQL avec encodage UTF-8 forcé."""
    try:
        os.environ['PGCLIENTENCODING'] = 'UTF8'  # Force l'encodage client
        
        conn = psycopg2.connect(
            host=host, 
            port=port, 
            dbname=database, 
            user=user
        )
        conn.set_client_encoding('UTF8')  # Assure la cohérence côté client
        return conn
    except UnicodeDecodeError:
        # En cas d'erreur d'encodage, tente une connexion sans forcer l'encodage (solution de secours)
        conn = psycopg2.connect(
            host=host, 
            port=port, 
            dbname=database, 
            user=user
        )
        return conn

def create_deck_match_table(conn):
    """Supprime la table 'deck_match' si elle existe et en crée une nouvelle en mode UNLOGGED (insertion plus rapide, mais pas durable en cas de crash)."""
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS deck_match;
            CREATE UNLOGGED TABLE deck_match (
                id SERIAL,                   -- Clé primaire auto-incrémentée
                match_id INT NOT NULL,       -- Référence au match
                player_id TEXT NOT NULL,     -- Identifiant du joueur
                deck_id TEXT,                -- Identifiant du deck (player_id + tournoi)
                wins SMALLINT DEFAULT 0,     -- Nombre de victoires sur ce match
                draws SMALLINT DEFAULT 0,    -- Nombre de matchs nuls
                losses SMALLINT DEFAULT 0    -- Nombre de défaites
            );
        """)
        conn.commit()
        print("[OK] Table UNLOGGED 'deck_match' créée.")

def populate_deck_match_ultra_fast(conn):
    """Insertion ultra-rapide des données dans deck_match à partir de la table match.
    
    La colonne deck_id est créée dynamiquement en concaténant player_id et tournament_id.
    """
    start_time = time.time()
    
    print("[INFO] Insertion directe avec SQL pur...")
    print("[INFO] Utilisation du deck_id = player_id + '_' + tournament_id")
    
    with conn.cursor() as cur:
        # Requête SQL insérant les données dans deck_match avec calcul des victoires, nuls, et défaites
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
        
        total_inserted = cur.rowcount  # Nombre total de lignes insérées
        conn.commit()
        
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        
        print(f"[OK] {total_inserted:,} lignes insérées en {elapsed:.1f}s ({rate:,.0f} lignes/sec)")
        
        # Finalisation : passage de la table en mode LOGGED, ajout d'une clé primaire et d'indexes pour accélérer les requêtes futures
        print("[INFO] Finalisation...")
        cur.execute("ALTER TABLE deck_match SET LOGGED")
        cur.execute("ALTER TABLE deck_match ADD PRIMARY KEY (id)")
        cur.execute("CREATE INDEX idx_deck_match_deck_id ON deck_match(deck_id)")
        cur.execute("CREATE INDEX idx_deck_match_player_id ON deck_match(player_id)")
        cur.execute("CREATE INDEX idx_deck_match_match_id ON deck_match(match_id)")
        conn.commit()

def main():
    """Point d'entrée principal du script."""
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
