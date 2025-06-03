import psycopg2
import psycopg2.extras
import re

host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'

def extract_pokemon_names(deck_comp):
    # deck_comp est une string comme "Pikachu x2, Bulbizarre x1"
    # on veut ['Pikachu', 'Bulbizarre']
    noms = []
    for part in deck_comp.split(','):
        nom = part.strip()
        nom_clean = re.sub(r'\s*x\d+$', '', nom)
        if nom_clean:
            noms.append(nom_clean)
    return noms

def add_deck_nom_column(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name='deck' AND column_name='deck_nom';
        """)
        if not cur.fetchone():
            cur.execute("ALTER TABLE deck ADD COLUMN deck_nom TEXT;")
            conn.commit()
            print("✅ Colonne 'deck_nom' ajoutée.")
        else:
            print("ℹ️ Colonne 'deck_nom' existe déjà.")

def update_deck_nom(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        print("📥 Récupération des decks pour mise à jour de deck_nom...")
        cur.execute("SELECT deck_id, deck_comp FROM deck;")
        decks = cur.fetchall()

        # Préparer statements pour éviter répétition
        cur.prepare_id_card = conn.cursor()
        cur.prepare_is_final = conn.cursor()

        update_data = []

        total = len(decks)
        print(f"🔎 {total} decks à traiter...")

        for idx, deck in enumerate(decks, start=1):
            deck_id = deck['deck_id']
            deck_comp = deck['deck_comp'] or ''
            noms_pokemon = extract_pokemon_names(deck_comp)
            final_pokemons = []

            for nom in noms_pokemon:
                # Recherche id_card
                cur.prepare_id_card.execute(
                    "SELECT card_id FROM card WHERE card_name = %s", (nom,))
                res = cur.prepare_id_card.fetchone()
                if res:
                    id_card = res[0]
                    # Vérifier poke_finale = '1'
                    cur.prepare_is_final.execute(
                        "SELECT 1 FROM card_evolve WHERE card_id = %s AND card_poke_finale = '1'", (id_card,))
                    if cur.prepare_is_final.fetchone():
                        final_pokemons.append(nom)

            deck_nom = ', '.join(final_pokemons) if final_pokemons else None
            update_data.append((deck_nom, deck_id))

            if idx % 100 == 0 or idx == total:
                print(f"🔄 Traitement deck {idx}/{total}...")

        # Mise à jour en batch
        with conn.cursor() as update_cur:
            psycopg2.extras.execute_batch(update_cur,
                "UPDATE deck SET deck_nom = %s WHERE deck_id = %s",
                update_data,
                page_size=500)
            conn.commit()

        cur.prepare_id_card.close()
        cur.prepare_is_final.close()

        print(f"✅ Mise à jour de 'deck_nom' terminée pour {total} decks.")

def main():
    try:
        with psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user
        ) as conn:
            conn.set_client_encoding('UTF8')
            add_deck_nom_column(conn)
            update_deck_nom(conn)
        print("🏁 Script terminé avec succès.")
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == '__main__':
    main()
