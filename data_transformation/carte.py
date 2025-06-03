import psycopg2
import psycopg2.extras
import json
import re
import os

# Connexion PostgreSQL
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'

# Dossier contenant les fichiers JSON
json_folder = r"E:\DataCollection\output"  # adapte ton chemin si besoin

def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]', ' ', text).strip()

def drop_and_create_card_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS card CASCADE;")
        cur.execute("""
            CREATE TABLE card (
                card_id TEXT PRIMARY KEY,
                card_name TEXT,
                card_type TEXT
            );
        """)
        conn.commit()
        print("✅ Table 'card' créée.")

def collect_cards_from_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            if not isinstance(data, dict) or 'players' not in data:
                return []
            cards = []
            for player in data['players']:
                for card in player.get('decklist', []):
                    if isinstance(card, dict) and card.get('url') and card.get('name') and card.get('type'):
                        cards.append((
                            remove_non_ascii(card['url']),
                            remove_non_ascii(card['name']),
                            remove_non_ascii(card['type'])
                        ))
            return cards
        except Exception:
            return []

def insert_cards_batch(conn, cards):
    if not cards:
        return 0
    with conn.cursor() as cur:
        sql = """
            INSERT INTO card (card_id, card_name, card_type)
            VALUES %s
            ON CONFLICT (card_id) DO NOTHING
        """
        psycopg2.extras.execute_values(cur, sql, cards, page_size=1000)
    return len(cards)

def main():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            options="-c client_encoding=utf8"
        )

        drop_and_create_card_table(conn)

        total_files = 0
        total_cards = 0
        inserted_cards = 0

        for filename in os.listdir(json_folder):
            if filename.endswith('.json'):
                total_files += 1
                path = os.path.join(json_folder, filename)
                cards = collect_cards_from_json(path)
                total_cards += len(cards)
                inserted = insert_cards_batch(conn, cards)
                inserted_cards += inserted
                print(f"✅ {filename} : {inserted} / {len(cards)} cartes insérées")

        conn.commit()
        conn.close()
        print(f"\n✅ Terminé : {inserted_cards} cartes insérées sur {total_cards} scannées dans {total_files} fichiers.")

    except Exception as e:
        print(f"❌ Erreur générale : {e}")

if __name__ == '__main__':
    main()
