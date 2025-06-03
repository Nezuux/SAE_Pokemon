import psycopg2
import psycopg2.extras
import json
import re
import os

# Paramètres PostgreSQL
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'

# Dossier contenant les fichiers JSON
json_folder = r"E:\DataCollection\output"  # à adapter selon ton environnement

def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]', ' ', text).strip() if text else ''

def drop_and_create_participation_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS participation;")
        cur.execute("""
            CREATE TABLE participation (
                participation_id SERIAL PRIMARY KEY,
                player_id TEXT REFERENCES player(player_id),
                player_name TEXT,
                tournament_id TEXT REFERENCES tournament(tournament_id),
                tournament_name TEXT,
                participation_placing INT
            );
        """)
        conn.commit()
        print("✅ Table 'participation' supprimée et recréée.")

def collect_participations_from_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            tournament_id = remove_non_ascii(data.get('id'))
            tournament_name = remove_non_ascii(data.get('name'))
            participations = []
            seen = set()
            for joueur in data.get('players', []):
                player_id = remove_non_ascii(joueur.get('id'))
                player_name = remove_non_ascii(joueur.get('name'))
                placing = joueur.get('placing')
                if placing is not None and str(placing).isdigit() and int(placing) > 0:
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
        except Exception as e:
            print(f"❌ Erreur de lecture JSON : {file_path} → {e}")
            return []

def insert_participations_batch(conn, participations):
    if not participations:
        return 0
    with conn.cursor() as cur:
        sql = """
            INSERT INTO participation
            (player_id, player_name, tournament_id, tournament_name, participation_placing)
            VALUES %s
            ON CONFLICT DO NOTHING;
        """
        psycopg2.extras.execute_values(cur, sql, participations, page_size=1000)
    return len(participations)

def main():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            options="-c client_encoding=utf8"
        )

        drop_and_create_participation_table(conn)

        total_files = 0
        total_participations = 0
        all_participations = []

        for filename in os.listdir(json_folder):
            if filename.endswith('.json'):
                total_files += 1
                path = os.path.join(json_folder, filename)
                participations = collect_participations_from_json(path)
                all_participations.extend(participations)
                total_participations += len(participations)
                print(f"✅ {filename} : {len(participations)} participations collectées")

        inserted = insert_participations_batch(conn, all_participations)
        conn.commit()
        conn.close()

        print(f"\n✅ Terminé : {inserted} participations insérées sur {total_participations} détectées dans {total_files} fichiers.")

    except Exception as e:
        print(f"❌ Erreur générale : {e}")

if __name__ == '__main__':
    main()
