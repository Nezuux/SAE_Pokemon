# -*- coding: utf-8 -*-
import sys
import os

# 🔧 Force l'encodage UTF-8 dès le départ pour stdout/stderr
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import psycopg2
import psycopg2.extras
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
import time
import multiprocessing

# ⚙️ Paramètres de connexion & performance
host = 'localhost'
port = 5432
database = 'postgres'
user = 'postgres'
json_folder = r"E:\DataCollection\output"  # Dossier contenant les fichiers JSON
BATCH_SIZE = 500000                        # Taille de batch énorme pour insertions bulk
MAX_WORKERS = min(16, multiprocessing.cpu_count())  # Auto-adaptation au nombre de cœurs dispo

# 📌 Regex précompilées pour nettoyage rapide
ASCII_PATTERN = re.compile(r'[^\x00-\x7F]')          # Filtre les caractères non ASCII
QUANTITY_PATTERN = re.compile(r'\s*x\d+$')           # Supprime les suffixes type " x4"

# 🔠 Nettoyage rapide d’un texte
def clean_text(text):
    if not isinstance(text, str):
        return ''
    return ASCII_PATTERN.sub(' ', text).strip()

# 📥 Chargement JSON blindé multi-encodage
def safe_json_load(file_path):
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                return json.load(f)
        except:
            continue
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
            text = content.decode('utf-8', errors='replace')
            return json.loads(text)
    except:
        return None

# 📂 Listage sécurisé des fichiers JSON du dossier
def safe_listdir(folder):
    try:
        return [f for f in os.listdir(folder) if f.endswith('.json')]
    except:
        return []

# 🔌 Connexion PostgreSQL robuste
def get_conn():
    try:
        os.environ['PGCLIENTENCODING'] = 'UTF8'
        conn = psycopg2.connect(
            host=host, port=port, dbname=database, user=user
        )
        conn.set_client_encoding('UTF8')
        return conn
    except UnicodeDecodeError:
        return psycopg2.connect(
            host=host, port=port, dbname=database, user=user
        )

# 🆔 Génère un identifiant unique de deck basé sur joueur + tournoi
def create_deck_id(player_id, tournament_id):
    return f"{player_id}_{tournament_id}"

# 🧱 Création de la table deck en mode UNLOGGED (insertion plus rapide)
def create_deck_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS deck;
            CREATE UNLOGGED TABLE deck (
                deck_id TEXT PRIMARY KEY,
                player_id TEXT,
                tournament_id TEXT,
                deck_comp TEXT,
                deck_nom TEXT
            );
        """)
    conn.commit()
    print("[OK] Table UNLOGGED 'deck' créée.")

# 🔍 Extraction de noms de Pokémon depuis deck_comp
def extract_pokemon_names(deck_comp):
    if not deck_comp:
        return []
    return [
        QUANTITY_PATTERN.sub('', part.strip())
        for part in deck_comp.split(',')
        if part.strip()
    ]

# 🔄 Traite un chunk de fichiers JSON pour extraire les decks
def process_file_chunk(filenames):
    all_decks = []
    for filename in filenames:
        try:
            file_path = os.path.join(json_folder, filename)
            data = safe_json_load(file_path)
            if not data:
                continue
            tournament_id = clean_text(data.get('id', ''))
            if not tournament_id:
                continue
            for player in data.get('players', []):
                player_id = clean_text(player.get('id', ''))
                decklist = player.get('decklist', [])
                if not decklist or not player_id:
                    continue
                deck_id = create_deck_id(player_id, tournament_id)
                card_names = [
                    clean_text(card.get('name', ''))
                    for card in decklist
                    if card.get('name')
                ]
                if card_names:
                    deck_comp = ', '.join(sorted(card_names))
                    all_decks.append((deck_id, player_id, tournament_id, deck_comp))
        except:
            continue
    return all_decks

# 🔁 Découpe la liste des fichiers en morceaux
def chunked_files(files, chunk_size):
    for i in range(0, len(files), chunk_size):
        yield files[i:i + chunk_size]

# 🔁 Découpe un itérable générique en morceaux
def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

# 🚀 Insertion rapide de tous les decks en utilisant le parallélisme
def insert_decks_ultra_fast(conn):
    start_time = time.time()
    files = safe_listdir(json_folder)
    total_files = len(files)

    print(f"[INFO] Traitement de {total_files} fichiers avec {MAX_WORKERS} workers...")
    print("[INFO] deck_id = player_id + '_' + tournament_id")

    if not files:
        print("[INFO] Aucun fichier à traiter")
        return 0

    all_decks = []
    files_per_worker = max(1, total_files // MAX_WORKERS)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        file_chunks = list(chunked_files(files, files_per_worker))
        futures = [executor.submit(process_file_chunk, chunk) for chunk in file_chunks]
        processed_chunks = 0
        for future in as_completed(futures):
            decks = future.result()
            if decks:
                all_decks.extend(decks)
            processed_chunks += 1
            print(f"[PARALLEL] {processed_chunks}/{len(file_chunks)} chunks traités | {len(all_decks):,} decks collectés")

    total_decks = len(all_decks)
    print(f"[INFO] {total_decks:,} decks collectés, début insertion...")

    if not all_decks:
        print("[INFO] Aucun deck trouvé")
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for chunk_num, chunk in enumerate(chunked_iterable(all_decks, BATCH_SIZE)):
            try:
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO deck (deck_id, player_id, tournament_id, deck_comp)
                       VALUES %s
                       ON CONFLICT (deck_id) DO UPDATE SET
                       player_id = EXCLUDED.player_id,
                       tournament_id = EXCLUDED.tournament_id,
                       deck_comp = EXCLUDED.deck_comp""",
                    chunk,
                    page_size=BATCH_SIZE
                )
                inserted += len(chunk)
                if chunk_num % 2 == 0:
                    print(f"[INSERTION] {inserted:,}/{total_decks:,} decks insérés", end='\r')
            except Exception as e:
                print(f"\n[ERREUR INSERTION] {e}")
                continue

    conn.commit()
    elapsed = time.time() - start_time
    rate = total_decks / elapsed if elapsed > 0 else 0
    print(f"\n[OK] {total_decks:,} decks insérés en {elapsed:.1f}s ({rate:,.0f} decks/sec)")
    return total_decks

# 🧠 Enrichissement des noms de deck (deck_nom) par SQL pur
def enrich_deck_nom_ultra_optimized(conn):
    start_time = time.time()
    print("[INFO] Enrichissement avec SQL pur...")

    with conn.cursor() as cur:
        try:
            cur.execute("""
                UPDATE deck 
                SET deck_nom = (
                    SELECT string_agg(DISTINCT c.card_name, ', ' ORDER BY c.card_name)
                    FROM unnest(string_to_array(deck.deck_comp, ', ')) AS deck_card_name
                    JOIN card c ON c.card_name = trim(deck_card_name)
                    WHERE c.card_id IN (
                        SELECT card_id 
                        FROM card_evolve 
                        WHERE card_poke_finale = 1
                    )
                )
                WHERE deck_comp IS NOT NULL;
            """)
            updated_rows = cur.rowcount
            conn.commit()
            elapsed = time.time() - start_time
            print(f"[OK] {updated_rows:,} decks enrichis en {elapsed:.1f}s")
        except Exception as e:
            print(f"[INFO] Enrichissement SQL échoué, fallback Python : {e}")
            enrich_deck_nom_fallback(conn)

# 🛠️ Fallback : enrichissement en chunks avec Python
def enrich_deck_nom_fallback(conn):
    start_time = time.time()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        print("[INFO] Enrichissement par chunks...")

        try:
            cur.execute("SELECT card_id, card_name FROM card;")
            card_name_to_id = {row['card_name']: row['card_id'] for row in cur.fetchall()}
            cur.execute("SELECT card_id FROM card_evolve WHERE card_poke_finale = 1;")
            final_poke_ids = set(row['card_id'] for row in cur.fetchall())
        except Exception as e:
            print(f"[INFO] Tables de référence non disponibles : {e}")
            return

        cur.execute("SELECT COUNT(*) FROM deck WHERE deck_comp IS NOT NULL;")
        total_decks = cur.fetchone()['count']

        processed = 0
        chunk_size = 50000
        for offset in range(0, total_decks, chunk_size):
            cur.execute("""
                SELECT deck_id, deck_comp 
                FROM deck 
                WHERE deck_comp IS NOT NULL
                ORDER BY deck_id 
                LIMIT %s OFFSET %s
            """, (chunk_size, offset))
            decks_chunk = cur.fetchall()
            update_data = []
            for deck in decks_chunk:
                noms_pokemon = extract_pokemon_names(deck['deck_comp'] or '')
                final_pokemons = [
                    nom for nom in noms_pokemon
                    if card_name_to_id.get(nom) in final_poke_ids
                ]
                deck_nom = ', '.join(final_pokemons) if final_pokemons else None
                update_data.append((deck_nom, deck['deck_id']))
            if update_data:
                try:
                    psycopg2.extras.execute_batch(
                        cur,
                        "UPDATE deck SET deck_nom = %s WHERE deck_id = %s",
                        update_data,
                        page_size=10000
                    )
                except Exception as e:
                    print(f"[ERREUR UPDATE] {e}")
            processed += len(decks_chunk)
            print(f"[ENRICHISSEMENT] {processed:,}/{total_decks:,} decks traités", end='\r')

    conn.commit()
    elapsed = time.time() - start_time
    print(f"\n[OK] Enrichissement terminé en {elapsed:.1f}s")

# 🚨 Point d’entrée du script
def main():
    start_time = time.time()
    try:
        print("[INFO] Lancement du script ULTRA-RAPIDE...")
        conn = get_conn()
        create_deck_table(conn)
        total_decks = insert_decks_ultra_fast(conn)
        enrich_deck_nom_ultra_optimized(conn)

        # 🔚 Finalisation : table LOGGED + index
        print("[INFO] Finalisation...")
        with conn.cursor() as cur:
            try:
                cur.execute("ALTER TABLE deck SET LOGGED")
                cur.execute("CREATE INDEX idx_deck_player ON deck(player_id)")
                cur.execute("CREATE INDEX idx_deck_tournament ON deck(tournament_id)")
                cur.execute("CREATE INDEX idx_deck_nom ON deck(deck_nom)")
                conn.commit()
            except Exception as e:
                print(f"[ERREUR FINALISATION] {e}")

        conn.close()
        elapsed = time.time() - start_time
        rate = total_decks / elapsed if elapsed > 0 else 0
        print(f"\n[OK] ULTRA-RAPIDE terminé en {elapsed:.1f}s : {total_decks:,} decks traités")
        print(f"[PERFORMANCE] {rate:,.0f} decks/sec")

    except Exception as e:
        print(f"[ERREUR CRITIQUE] {e}")

# ▶️ Lancement
if __name__ == '__main__':
    main()
