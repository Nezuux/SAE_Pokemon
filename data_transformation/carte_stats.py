import psycopg2
import json
import re
import os

host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'
json_folder = r"D:\DataCollection\output"
batch_size = 500  # Pour tuning perf

def remove_non_ascii(text):
    return re.sub(r'[^\x00-\x7F]', ' ', text).strip()

def drop_and_create_deck_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS Deck;")
        cur.execute("""
            CREATE TABLE Deck (
                deck_id TEXT,
                id_tournament TEXT,
                id_player TEXT REFERENCES Joueur(id),
                id_card TEXT REFERENCES Carte(id_card),
                count INT,
                deck_name TEXT,
                PRIMARY KEY (deck_id, id_card)
            );
        """)
    conn.commit()
    print("✅ Table 'Deck' recréée.")

def load_carte_lookup(conn):
    lookup = {}
    with conn.cursor() as cur:
        cur.execute("SELECT name_card, id_card, type FROM Carte;")
        for name_card, id_card, type_ in cur.fetchall():
            key = (remove_non_ascii(name_card).lower(), remove_non_ascii(id_card), remove_non_ascii(type_).lower())
            lookup[key] = id_card
    print(f"✅ Lookup cartes chargé : {len(lookup)} entrées.")
    return lookup

def get_id_card(carte, lookup, conn):
    name = remove_non_ascii(carte['name']).lower()
    url = remove_non_ascii(carte['url'])
    type_ = remove_non_ascii(carte['type']).lower()

    key = (name, url, type_)
    if key in lookup:
        return lookup[key]

    # Fallback en base de données
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id_card FROM Carte
            WHERE id_card = %s AND type = %s AND name_card ILIKE %s;
        """, (url, carte['type'], f"%{carte['name']}%"))
        row = cur.fetchone()
        return row[0] if row else None

def insert_decks_batch(conn, rows):
    with conn.cursor() as cur:
        args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s)", row).decode('utf-8') for row in rows)
        cur.execute(f"""
            INSERT INTO Deck (deck_id, id_tournament, id_player, id_card, count)
            VALUES {args_str}
            ON CONFLICT (deck_id, id_card) DO UPDATE SET count = EXCLUDED.count;
        """)
    conn.commit()
    print(f"✅ Batch inséré : {len(rows)} lignes.")

def update_all_deck_names(conn):
    with conn.cursor() as cur:
        cur.execute("""
            WITH deck_names AS (
                SELECT
                    d.deck_id,
                    string_agg(DISTINCT c.name_card, ' + ' ORDER BY c.name_card) AS deck_name
                FROM Deck d
                JOIN Carte c ON d.id_card = c.id_card
                JOIN evolution e ON c.id_card = e.url_carte
                WHERE e.poke_finale = '1'
                GROUP BY d.deck_id
            )
            UPDATE Deck d2
            SET deck_name = dn.deck_name
            FROM deck_names dn
            WHERE d2.deck_id = dn.deck_id;
        """)
    conn.commit()
    print("✅ Noms des decks mis à jour.")

def calculate_win_rates_by_extension(conn):
    print("\n🚀 Calcul des win rates par carte et par extension...")

    query = """
    WITH extensions_periods AS (
        SELECT
            numero_extension,
            extension,
            date_sortie,
            LEAD(date_sortie) OVER (ORDER BY date_sortie) AS next_date_sortie
        FROM carte_extensions
    ),
    matches_with_extension AS (
        SELECT
            mr.*,
            t.date AS tournament_date,
            ep.numero_extension
        FROM match_result mr
        JOIN tournois t ON mr.tournament_id = t.id
        JOIN extensions_periods ep
          ON t.date >= ep.date_sortie
         AND (t.date < ep.next_date_sortie OR ep.next_date_sortie IS NULL)
    ),
    card_usage_in_matches AS (
        SELECT
            mw.numero_extension,
            d.id_card,
            COUNT(*) FILTER (WHERE mw.winner_id = d.id_player) AS wins,
            COUNT(*) AS total_matches
        FROM matches_with_extension mw
        JOIN Deck d ON d.deck_id = CONCAT(mw.tournament_id, '#', d.id_player)
        GROUP BY mw.numero_extension, d.id_card
    )
    SELECT
        id_card,
        numero_extension,
        wins,
        total_matches,
        ROUND(COALESCE(wins::decimal, 0) / NULLIF(total_matches, 0), 3) AS win_rate
    FROM card_usage_in_matches
    ORDER BY id_card, numero_extension;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
        print("✅ Requête exécutée avec succès.\n")

        # Affichage simple
        for id_card, numero_extension, wins, total_matches, win_rate in results:
            print(f"Carte {id_card} - Extension {numero_extension} : {wins} victoires / {total_matches} matchs -> Win Rate = {win_rate:.3f}")

        print("\n✅ Calcul des win rates terminé.")
    except Exception as e:
        print(f"❌ Erreur lors du calcul des win rates : {e}")

def main():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            options="-c client_encoding=utf8"
        )

        drop_and_create_deck_table(conn)
        carte_lookup = load_carte_lookup(conn)

        total_files = 0
        total_inserted = 0
        total_missing = 0
        batch_rows = []

        for filename in os.listdir(json_folder):
            if filename.endswith('.json'):
                total_files += 1
                path = os.path.join(json_folder, filename)
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        id_tournament = data['id']
                        for joueur in data.get('players', []):
                            id_player = remove_non_ascii(joueur['id'])
                            deck_id = f"{id_tournament}#{id_player}"
                            for carte in joueur.get('decklist', []):
                                id_card = get_id_card(carte, carte_lookup, conn)
                                if id_card:
                                    row = (
                                        deck_id,
                                        id_tournament,
                                        id_player,
                                        id_card,
                                        int(carte['count'])
                                    )
                                    batch_rows.append(row)
                                    print(f"✅ {deck_id} → {carte['name']} x{carte['count']}")
                                    total_inserted += 1
                                else:
                                    print(f"⚠️ Carte introuvable : {carte['name']}")
                                    total_missing += 1

                            if len(batch_rows) >= batch_size:
                                insert_decks_batch(conn, batch_rows)
                                batch_rows.clear()

                except Exception as e:
                    print(f"❌ Erreur fichier {filename} : {e}")

        if batch_rows:
            insert_decks_batch(conn, batch_rows)

        update_all_deck_names(conn)

        # Appel de la fonction de calcul des stats
        calculate_win_rates_by_extension(conn)

        conn.close()

        print(f"\n✅ {total_inserted} cartes insérées depuis {total_files} fichiers.")
        if total_missing > 0:
            print(f"⚠️ {total_missing} cartes manquantes (non trouvées dans Carte).")

    except Exception as e:
        print(f"❌ Erreur générale : {e}")

if __name__ == '__main__':
    main()
