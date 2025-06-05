# -*- coding: utf-8 -*-
import sys
import os

# Forcer l'encodage de la sortie standard et d'erreur en UTF-8 (utile pour les consoles Windows)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import psycopg2
import json
import re

# Paramètres de connexion PostgreSQL
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'

# Dossier contenant les fichiers JSON à traiter
json_folder = os.getenv("JSON_FOLDER", r"E:\DataCollection\output")

def remove_non_ascii(text):
    """
    Supprime les caractères non-ASCII d'une chaîne de texte.
    Remplace les caractères invalides par un espace.
    """
    if text is None:
        return ''
    return re.sub(r'[^\x00-\x7F]', ' ', str(text)).strip()

def safe_json_load(file_path):
    """
    Tente de charger un fichier JSON en testant plusieurs encodages courants.
    Retourne None en cas d'échec.
    """
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                return json.load(f)
        except:
            continue
    
    # Dernière tentative avec lecture binaire et décodage manuel
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
            text = content.decode('utf-8', errors='replace')
            return json.loads(text)
    except:
        return None

def safe_listdir(folder):
    """
    Liste les fichiers .json d’un dossier en toute sécurité.
    Évite les erreurs dues à des dossiers manquants ou protégés.
    """
    try:
        return [f for f in os.listdir(folder) if f.endswith('.json')]
    except:
        return []

def get_conn():
    """
    Établit une connexion PostgreSQL avec encodage UTF-8.
    Gère les erreurs liées à l'encodage.
    """
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
        # Connexion de secours si l'encodage échoue
        conn = psycopg2.connect(
            host=host, 
            port=port, 
            dbname=database, 
            user=user
        )
        return conn

def create_player_table(conn):
    """
    Crée la table 'player' si elle n’existe pas déjà.
    """
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS player (
            player_id TEXT PRIMARY KEY,
            player_name TEXT,
            player_country TEXT
        );
        """)
        conn.commit()
        print("✅ Table 'player' prête.")

def upsert_player(conn, player):
    """
    Insère ou met à jour un joueur dans la table 'player'.
    Gère les doublons via ON CONFLICT.
    """
    with conn.cursor() as cur:
        try:
            player_id = player.get('id') or ''
            player_name = player.get('name') or ''
            player_country = player.get('country') or ''
            
            cur.execute("""
            INSERT INTO player (player_id, player_name, player_country)
            VALUES (%s, %s, %s)
            ON CONFLICT (player_id) DO UPDATE
            SET player_name = EXCLUDED.player_name,
                player_country = EXCLUDED.player_country
            RETURNING player_id;
            """, (
                remove_non_ascii(player_id),
                remove_non_ascii(player_name),
                remove_non_ascii(player_country)
            ))
            return cur.fetchone() is not None
        except Exception as e:
            print(f"❌ Erreur upsert player {player.get('name', 'inconnu')} : {e}")
            return False

def main():
    """
    Programme principal : lit les fichiers JSON, insère les joueurs en base de données,
    et affiche un rapport final.
    """
    try:
        print("[INFO] Démarrage du traitement players...")
        
        conn = get_conn()
        create_player_table(conn)

        files = safe_listdir(json_folder)
        total_files = len(files)
        total_players = 0
        upserted_players = 0

        print(f"[INFO] {total_files} fichiers trouvés")

        for i, filename in enumerate(files):
            if i % 100 == 0:
                print(f"[PROGRESS] {i}/{total_files} fichiers traités")
            
            path = os.path.join(json_folder, filename)
            data = safe_json_load(path)
            
            if not data:
                continue
                
            for player in data.get('players', []):
                # Ne traite que les joueurs avec un id et un nom valides
                if player.get('id') and player.get('name'):
                    total_players += 1
                    if upsert_player(conn, player):
                        upserted_players += 1
            
            # Commit périodique pour limiter la taille des transactions
            if i % 50 == 0:
                conn.commit()

        conn.commit()
        conn.close()
        print(f"\n✅ Terminé : {upserted_players} players insérés ou mis à jour sur {total_players} dans {total_files} fichiers.")

    except Exception as e:
        print(f"❌ Erreur générale : {e}")

if __name__ == '__main__':
    main()
