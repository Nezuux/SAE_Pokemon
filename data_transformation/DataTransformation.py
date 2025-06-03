import psycopg2
import json
import os
import re

# Connexion PostgreSQL
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'
json_folder = r"E:\DataCollection\output"  # adapte ton chemin

def clean(text):
    return re.sub(r'[^\x00-\x7F]', ' ', text).strip()

def connect_db():
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        options="-c client_encoding=utf8"
    )

def drop_and_create_all_tables(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Deck CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Match_result CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Carte CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Joueur CASCADE;")
        cur.execute("DROP TABLE IF EXISTS Tournois CASCADE;")

        cur.execute("""
            CREATE TABLE Tournois (
                id TEXT PRIMARY KEY,
                name TEXT,
                "date" TIMESTAMP,
                organizer TEXT,
                "format" TEXT,
                nb_players INT
            );
        """)
        cur.execute("""
            CREATE TABLE Joueur (
                id TEXT PRIMARY KEY,
                name TEXT,
                "placing" INT,
                country TEXT,
                tournament_id TEXT REFERENCES Tournois(id)
            );
        """)
        cur.execute("""
            CREATE TABLE Carte (
                id SERIAL PRIMARY KEY,
                name TEXT,
                url TEXT,
                type TEXT,
                UNIQUE(name, url, type)
            );
        """)
        cur.execute("""
            CREATE TABLE Deck (
                id SERIAL PRIMARY KEY,
                player_id TEXT REFERENCES Joueur(id),
                card_id INT REFERENCES Carte(id),
                count INT
            );
        """)
        cur.execute("""
            CREATE TABLE Match_result (
                id SERIAL PRIMARY KEY,
                tournament_id TEXT REFERENCES Tournois(id),
                player1_id TEXT,
                player1_score INT,
                player2_id TEXT,
                player2_score INT
            );
        """)
        conn.commit()
        print("✅ Toutes les tables ont été recréées.")

def insert_tournoi(cur, data):
    cur.execute("""
        INSERT INTO Tournois (id, name, "date", organizer, "format", nb_players)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """, (
        clean(data['id']), clean(data['name']), clean(data['date']),
        clean(data['organizer']), clean(data['format']), int(data['nb_players'])
    ))

def insert_joueur(cur, joueur, tournoi_id):
    cur.execute("""
        INSERT INTO Joueur (id, name, "placing", country, tournament_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """, (
        clean(joueur['id']), clean(joueur['name']), int(joueur['placing']),
        clean(joueur.get('country') or ''), tournoi_id
    ))

def insert_carte_get_id(cur, carte):
    cur.execute("""
        INSERT INTO Carte (name, url, type)
        VALUES (%s, %s, %s)
        ON CONFLICT (name, url, type) DO NOTHING
        RETURNING id;
    """, (
        clean(carte['name']), clean(carte['url']), clean(carte['type'])
    ))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("""
        SELECT id FROM Carte WHERE name=%s AND url=%s AND type=%s;
    """, (
        clean(carte['name']), clean(carte['url']), clean(carte['type'])
    ))
    row = cur.fetchone()
    return row[0] if row else None

def insert_deck(cur, player_id, decklist):
    for carte in decklist:
        card_id = insert_carte_get_id(cur, carte)
        if card_id:
            cur.execute("""
                INSERT INTO Deck (player_id, card_id, count)
                VALUES (%s, %s, %s);
            """, (player_id, card_id, int(carte['count'])))

def insert_match(cur, tournoi_id, matches):
    for match in matches:
        if 'match_results' in match and len(match['match_results']) == 2:
            p1 = match['match_results'][0]
            p2 = match['match_results'][1]
            cur.execute("""
                INSERT INTO Match_result (tournament_id, player1_id, player1_score, player2_id, player2_score)
                VALUES (%s, %s, %s, %s, %s);
            """, (
                clean(tournoi_id),
                clean(p1['player_id']), int(p1['score']),
                clean(p2['player_id']), int(p2['score'])
            ))

def main():
    try:
        conn = connect_db()
        drop_and_create_all_tables(conn)

        total_tournois = total_joueurs = total_cartes = total_decks = total_matches = 0

        for filename in os.listdir(json_folder):
            if filename.endswith('.json'):
                path = os.path.join(json_folder, filename)
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    with conn.cursor() as cur:
                        insert_tournoi(cur, data)
                        total_tournois += 1

                        for joueur in data.get('players', []):
                            insert_joueur(cur, joueur, clean(data['id']))
                            total_joueurs += 1
                            insert_deck(cur, joueur['id'], joueur.get('decklist', []))
                            total_decks += len(joueur.get('decklist', []))
                            total_cartes += len(joueur.get('decklist', []))

                        insert_match(cur, data['id'], data.get('matches', []))
                        total_matches += len(data.get('matches', []))

                        conn.commit()

        conn.close()

        print("\n✅ Import global terminé !")
        print(f"📦 {total_tournois} tournois")
        print(f"👥 {total_joueurs} joueurs")
        print(f"🃏 {total_cartes} cartes insérées via deck")
        print(f"📑 {total_decks} entrées deck")
        print(f"⚔️ {total_matches} matchs")

    except Exception as e:
        print(f"❌ Erreur globale : {e}")

if __name__ == '__main__':
    main()
