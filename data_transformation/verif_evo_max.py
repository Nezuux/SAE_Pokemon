import psycopg2

def fast_update_card_poke_finale():
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        dbname='postgres',
        user='postgres'
    )
    conn.set_client_encoding('UTF8')
    cur = conn.cursor()

    try:
        print("🔍 Vérification ou création/renommage de la colonne 'card_poke_finale'...")

        # Vérifier si la colonne card_poke_finale existe
        cur.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name='card_evolve' AND column_name='card_poke_finale';
        """)
        if not cur.fetchone():
            # Si card_poke_finale n'existe pas, vérifier si la colonne 'next' existe pour la renommer
            cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name='card_evolve' AND column_name='next';
            """)
            if cur.fetchone():
                cur.execute("ALTER TABLE card_evolve RENAME COLUMN next TO card_poke_finale;")
                print("✅ Colonne 'next' renommée en 'card_poke_finale'")
            else:
                cur.execute("ALTER TABLE card_evolve ADD COLUMN card_poke_finale INT DEFAULT 0;")
                print("✅ Colonne 'card_poke_finale' créée")
            conn.commit()

        print("📥 Chargement des données...")

        # Récupérer toutes les URLs distinctes dans card_previous_url (non NULL)
        cur.execute("SELECT DISTINCT card_previous_url FROM card_evolve WHERE card_previous_url IS NOT NULL;")
        previous_urls = set(row[0] for row in cur.fetchall())

        # Récupérer tous les id et card_id
        cur.execute("SELECT card_evolve_id, card_id FROM card_evolve;")
        rows = cur.fetchall()

        finale_ids = []
        non_finale_ids = []

        for row_id, card_id in rows:
            if card_id in previous_urls:
                non_finale_ids.append(row_id)
            else:
                finale_ids.append(row_id)

        print(f"🔁 {len(finale_ids)} finales / {len(non_finale_ids)} non-finales")

        # Mise à jour card_poke_finale à 1 pour finales
        if finale_ids:
            ids = tuple(finale_ids)
            if len(ids) == 1:
                ids = (ids[0],)
            cur.execute(
                "UPDATE card_evolve SET card_poke_finale = 1 WHERE card_evolve_id IN %s;",
                (ids,)
            )
        # Mise à jour card_poke_finale à 0 pour non-finales
        if non_finale_ids:
            ids = tuple(non_finale_ids)
            if len(ids) == 1:
                ids = (ids[0],)
            cur.execute(
                "UPDATE card_evolve SET card_poke_finale = 0 WHERE card_evolve_id IN %s;",
                (ids,)
            )

        conn.commit()
        print("✅ card_poke_finale mis à jour efficacement.")

    except Exception as e:
        print(f"💥 Erreur : {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        print("🔌 Connexion fermée.")

if __name__ == '__main__':
    fast_update_card_poke_finale()
