import psycopg2
import psycopg2.extras
import json
import os
import re

host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'

json_folder = r"E:\DataCollection\output"

def clean_text(text):
    if not isinstance(text, str):
        return ''
    return re.sub(r'[^\x00-\x7F]', ' ', text).strip()

def drop_and_create_deck_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS deck;")
        cur.execute("""
            CREATE TABLE deck (
                deck_id TEXT PRIMARY KEY,
                player_id TEXT,
                tournament_id TEXT,
                deck_comp TEXT
            );
        """)
    conn.commit()
    print("✅ Table 'deck' recréée.")

def batch_insert_decks(conn, decks):
    """
    Insert batch de decks.
    decks = liste de tuples (deck_id, player_id, tournament_id, deck_comp)
    """
    sql = """
        INSERT INTO deck (deck_id, player_id, tournament_id, deck_comp)
        VALUES %s
        ON CONFLICT (deck_id) DO UPDATE
        SET player_id = EXCLUDED.player_id,
            tournament_id = EXCLUDED.tournament_id,
            deck_comp = EXCLUDED.deck_comp;
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, decks, page_size=1000)
    conn.commit()

def main():
    print("🚀 Démarrage du script deck.py")
    try:
        with psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user
        ) as conn:
            conn.set_client_encoding('UTF8')
            drop_and_create_deck_table(conn)

            total_files = 0
            total_decks = 0
            decks_batch = []
            batch_size = 1000

            for filename in os.listdir(json_folder):
                if not filename.endswith('.json'):
                    continue
                total_files += 1
                path = os.path.join(json_folder, filename)
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    tournament_id = clean_text(str(data.get('id', '')))
                    for player in data.get('players', []):
                        player_id = clean_text(player.get('id', ''))
                        deck_id = f"{player_id}#{tournament_id}"

                        card_names = set()
                        for card in player.get('decklist', []):
                            nom = clean_text(card.get('name', ''))
                            if nom:
                                card_names.add(nom)

                        deck_comp = ', '.join(sorted(card_names))

                        decks_batch.append((deck_id, player_id, tournament_id, deck_comp))
                        total_decks += 1

                        if len(decks_batch) >= batch_size:
                            batch_insert_decks(conn, decks_batch)
                            print(f"📥 {total_decks} decks insérés jusqu'à présent...")
                            decks_batch.clear()

                except Exception as e:
                    print(f"❌ Erreur fichier {filename} : {e}")

            if decks_batch:
                batch_insert_decks(conn, decks_batch)

            print(f"\n🏁 Terminé : {total_decks} decks insérés depuis {total_files} fichiers.")

    except Exception as e:
        print(f"❌ Erreur générale : {e}")

if __name__ == '__main__':
    main()
