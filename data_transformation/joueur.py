import psycopg2
import json
import re
import os

# Paramètres PostgreSQL
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'

# Dossier contenant les fichiers JSON
json_folder = r"E:\DataCollection\output"  # à adapter à ton chemin

def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]', ' ', text)

def create_player_table(conn):
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
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO player (player_id, player_name, player_country)
        VALUES (%s, %s, %s)
        ON CONFLICT (player_id) DO UPDATE
        SET player_name = EXCLUDED.player_name,
            player_country = EXCLUDED.player_country
        RETURNING player_id;
        """, (
            remove_non_ascii(player['id']),
            remove_non_ascii(player['name']),
            remove_non_ascii(player.get('country') or '')
        ))
        return cur.fetchone() is not None

def main():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            options="-c client_encoding=utf8"
        )

        create_player_table(conn)

        total_files = 0
        total_players = 0
        upserted_players = 0

        for filename in os.listdir(json_folder):
            if filename.endswith('.json'):
                total_files += 1
                path = os.path.join(json_folder, filename)
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for player in data.get('players', []):
                            total_players += 1
                            if upsert_player(conn, player):
                                upserted_players += 1
                                print(f"✅ Player inséré/mis à jour : {player['name']}")
                    conn.commit()
                except Exception as file_err:
                    print(f"❌ Erreur dans {filename} : {file_err}")

        conn.close()
        print(f"\n✅ Terminé : {upserted_players} players insérés ou mis à jour sur {total_players} dans {total_files} fichiers.")

    except Exception as e:
        print(f"❌ Erreur générale : {e}")

if __name__ == '__main__':
    main()
