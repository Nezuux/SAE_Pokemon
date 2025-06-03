import psycopg2
import psycopg2.extras
import json
import os
import re

# Connexion PostgreSQL
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'

# Dossier contenant les fichiers JSON
json_folder = r"E:\DataCollection\output"  # adapte le chemin

def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]', ' ', text).strip()

def drop_and_create_match_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS match;")
        cur.execute("""
            CREATE TABLE match (
                match_id SERIAL PRIMARY KEY,
                tournament_id TEXT REFERENCES tournament(tournament_id),
                player1_id TEXT,
                player1_score INT,
                player2_id TEXT,
                player2_score INT,
                match_winner TEXT
            );
        """)
        conn.commit()
        print("✅ Table 'match' supprimée et recréée.")

def determine_winner(p1_score, p2_score, p1_id, p2_id):
    if p1_score > p2_score:
        return p1_id
    elif p2_score > p1_score:
        return p2_id
    else:
        return None  # match nul

def collect_matches_from_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            tournoi_id = remove_non_ascii(data['id'])
            matches = []
            for match in data.get('matches', []):
                if 'match_results' in match and len(match['match_results']) == 2:
                    p1 = match['match_results'][0]
                    p2 = match['match_results'][1]
                    p1_id = remove_non_ascii(p1['player_id'])
                    p2_id = remove_non_ascii(p2['player_id'])
                    p1_score = int(p1['score'])
                    p2_score = int(p2['score'])
                    winner = determine_winner(p1_score, p2_score, p1_id, p2_id)
                    matches.append((
                        tournoi_id,
                        p1_id,
                        p1_score,
                        p2_id,
                        p2_score,
                        winner
                    ))
            return matches
        except Exception:
            return []

def insert_matches_batch(conn, matches):
    if not matches:
        return 0
    with conn.cursor() as cur:
        sql = """
            INSERT INTO match (tournament_id, player1_id, player1_score, player2_id, player2_score, match_winner)
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        psycopg2.extras.execute_values(cur, sql, matches, page_size=1000)
    return len(matches)

def main():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            options="-c client_encoding=utf8"
        )

        drop_and_create_match_table(conn)

        total_files = 0
        total_matches = 0
        inserted_matches = 0

        all_matches = []
        for filename in os.listdir(json_folder):
            if filename.endswith('.json'):
                total_files += 1
                path = os.path.join(json_folder, filename)
                matches = collect_matches_from_json(path)
                total_matches += len(matches)
                all_matches.extend(matches)
                print(f"✅ {filename} : {len(matches)} matchs collectés")

        inserted_matches = insert_matches_batch(conn, all_matches)
        conn.commit()
        conn.close()

        print(f"\n✅ Terminé : {inserted_matches} matchs insérés sur {total_matches} détectés dans {total_files} fichiers.")

    except Exception as e:
        print(f"❌ Erreur générale : {e}")

if __name__ == '__main__':
    main()
