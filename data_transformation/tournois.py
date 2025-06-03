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
json_folder = r"E:\DataCollection\output"  # à adapter si besoin

def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]', ' ', text) if isinstance(text, str) else None

def create_tournament_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tournament (
                tournament_id TEXT PRIMARY KEY,
                tournament_name TEXT,
                tournament_date TIMESTAMP,
                tournament_organizer TEXT,
                tournament_format TEXT,
                tournament_nb_player INT,
                last_extension TEXT
            );
        """)
        conn.commit()
        print("✅ Table 'tournament' prête (avec colonne last_extension).")

def insert_tournaments_batch(conn, tournaments):
    """
    Insère un batch de tournois dans la table tournament.
    Ignore les doublons via ON CONFLICT DO NOTHING.
    """
    with conn.cursor() as cur:
        records = []
        for t in tournaments:
            date_val = t.get('date')
            records.append((
                remove_non_ascii(t.get('id')),
                remove_non_ascii(t.get('name')),
                date_val,
                remove_non_ascii(t.get('organizer')),
                remove_non_ascii(t.get('format')),
                int(t.get('nb_players', 0))
            ))
        sql = """
            INSERT INTO tournament (
                tournament_id, tournament_name, tournament_date,
                tournament_organizer, tournament_format, tournament_nb_player
            )
            VALUES %s
            ON CONFLICT (tournament_id) DO NOTHING
        """
        psycopg2.extras.execute_values(cur, sql, records, template=None, page_size=100)
    # Commit hors de cette fonction

def update_last_extension_for_tournaments(conn):
    with conn.cursor() as cur:
        # Récupérer la première extension (hors P-A), la plus ancienne
        cur.execute("""
            SELECT extension_code, extension_date_sortie
            FROM extension
            WHERE extension_code <> 'P-A'
            ORDER BY extension_date_sortie ASC
            LIMIT 1
        """)
        first_ext = cur.fetchone()
        if not first_ext:
            print("⚠️ Pas d'extensions valides trouvées dans la table extension.")
            return
        first_extension_code, first_extension_date = first_ext

        # Mise à jour last_extension en fonction des règles :
        # 1) Si tournament_date est NULL ou antérieure à la première extension,
        #    on met la première extension
        # 2) Sinon, on met la dernière extension sortie avant la date du tournoi (excluant 'P-A')
        sql = f"""
        UPDATE tournament t
        SET last_extension = sub.extension_code
        FROM (
            SELECT tournament_id, extension_code
            FROM (
                SELECT
                    t.tournament_id,
                    CASE
                        WHEN t.tournament_date IS NULL OR t.tournament_date < %s THEN %s
                        ELSE (
                            SELECT e.extension_code
                            FROM extension e
                            WHERE e.extension_date_sortie <= t.tournament_date::timestamp
                              AND e.extension_code <> 'P-A'
                            ORDER BY e.extension_date_sortie DESC
                            LIMIT 1
                        )
                    END AS extension_code,
                    ROW_NUMBER() OVER (PARTITION BY t.tournament_id ORDER BY t.tournament_date) as rn
                FROM tournament t
            ) AS sub1
            WHERE rn = 1
        ) AS sub
        WHERE t.tournament_id = sub.tournament_id;
        """
        cur.execute(sql, (first_extension_date, first_extension_code))
        conn.commit()
        print("✅ Colonne 'last_extension' mise à jour avec prise en compte des dates antérieures ou NULL.")

def main():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            options="-c client_encoding=utf8"
        )

        create_tournament_table(conn)

        total_files = 0
        total_inserted = 0

        for filename in os.listdir(json_folder):
            if filename.endswith('.json'):
                total_files += 1
                full_path = os.path.join(json_folder, filename)
                try:
                    with open(full_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        insert_tournaments_batch(conn, [data])
                        conn.commit()
                        print(f"✅ Tournoi inséré ou déjà existant : {data.get('name', 'N/A')}")
                        total_inserted += 1
                except Exception as file_err:
                    print(f"❌ Erreur dans le fichier {filename} : {file_err}")

        update_last_extension_for_tournaments(conn)

        conn.close()
        print(f"\n✅ Traitement terminé : {total_inserted} tournois traités sur {total_files} fichiers JSON.")

    except Exception as e:
        print(f"❌ Erreur générale : {e}")

if __name__ == '__main__':
    main()
