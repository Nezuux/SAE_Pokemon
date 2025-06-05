# -*- coding: utf-8 -*-
import sys
import os

# Force l'encodage UTF-8 dès le début
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import psycopg2
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
import re
import time

BATCH_SIZE = 50  # Taille des batchs d'insertion

def clean_text(text):
    if not text:
        return None
    return re.sub(r'[^\x00-\x7F]+', ' ', text).strip()

def fetch_evolution_from(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return (url, None)

        soup = BeautifulSoup(response.text, 'html.parser')
        link = soup.find('a', href=re.compile(r'^/cards\?q=name:'))
        if link and link.text.strip():
            return (url, clean_text(link.text))
    except Exception as e:
        print(f"❌ Exception fetch_evolution_from {url}: {e}")
    return (url, None)

def fetch_previous_urls(url):
    try:
        base_url = "https://pocket.limitlesstcg.com"
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        link_family = soup.find('a', href=re.compile(r'^/cards\?q=name:'))
        if not link_family or not link_family.has_attr('href'):
            return None

        family_url = urljoin(base_url, link_family['href'])
        fam_response = requests.get(family_url, timeout=10)
        if fam_response.status_code != 200:
            return None

        fam_soup = BeautifulSoup(fam_response.text, 'html.parser')
        links = fam_soup.find_all('a', href=re.compile(r'^/cards/[A-Za-z0-9]+/\d+'))

        return ",".join({urljoin(base_url, a['href']) for a in links if a.has_attr('href')}) or None
    except Exception as e:
        print(f"❌ Exception fetch_previous_urls {url}: {e}")
        return None

def get_conn():
    """Connexion PostgreSQL ultra-robuste"""
    try:
        os.environ['PGCLIENTENCODING'] = 'UTF8'
        
        conn = psycopg2.connect(
            host='localhost', 
            port=5432, 
            dbname='postgres', 
            user='postgres'
        )
        conn.set_client_encoding('UTF8')
        return conn
    except UnicodeDecodeError:
        conn = psycopg2.connect(
            host='localhost', 
            port=5432, 
            dbname='postgres', 
            user='postgres'
        )
        return conn

def update_card_poke_finale_optimized(conn):
    """Mise à jour optimisée de card_poke_finale"""
    cur = conn.cursor()
    
    try:
        print("🔍 Vérification ou création de la colonne 'card_poke_finale'...")

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

        print("📥 Mise à jour card_poke_finale avec SQL optimisé...")

        # Mise à jour ultra-rapide en SQL pur
        cur.execute("""
            UPDATE card_evolve 
            SET card_poke_finale = CASE 
                WHEN card_id IN (
                    SELECT DISTINCT card_previous_url 
                    FROM card_evolve 
                    WHERE card_previous_url IS NOT NULL
                ) THEN 0 
                ELSE 1 
            END;
        """)
        
        updated_rows = cur.rowcount
        conn.commit()
        print(f"✅ card_poke_finale mis à jour pour {updated_rows} lignes")

    except Exception as e:
        print(f"💥 Erreur mise à jour card_poke_finale : {e}")
        conn.rollback()
    finally:
        cur.close()

def update_card_evolve():
    conn = get_conn()
    cur = conn.cursor()

    try:
        # Création de la table
        cur.execute("DROP TABLE IF EXISTS card_evolve;")
        cur.execute("""
            CREATE TABLE card_evolve (
                card_evolve_id SERIAL PRIMARY KEY,
                card_id VARCHAR(255),
                card_previous_evolve VARCHAR(255),
                card_previous_url VARCHAR(255),
                card_poke_finale INT DEFAULT 0
            );
        """)
        conn.commit()
        print("✅ Table 'card_evolve' créée.")

        # Récupération des URL de cartes Pokémon
        cur.execute("SELECT card_id FROM card WHERE card_id IS NOT NULL AND card_type = 'Pok mon';")
        urls = [url for (url,) in cur.fetchall()]
        print(f"🔗 {len(urls)} cartes Pokémon à traiter...")

        if not urls:
            print("⚠️ Aucune carte Pokémon trouvée")
            return

        # Étape 1 : Récupération des évolutions
        print("🔍 Scraping card_previous_evolve...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_evolution_from, url) for url in urls]
            evo_from_results = {}
            for future in as_completed(futures):
                url, evo_from = future.result()
                evo_from_results[url] = evo_from

        # Étape 2 : Récupération des previous_urls + insertion en batch
        print("📥 Scraping card_previous_url et insertion...")
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures_prev = {executor.submit(fetch_previous_urls, url): url for url in urls}

            batch = []
            total_inserted = 0

            for future in as_completed(futures_prev):
                card_id = futures_prev[future]
                previous_urls_str = future.result()
                previous_urls = previous_urls_str.split(',') if previous_urls_str else [None]
                card_previous_evolve = evo_from_results.get(card_id)

                for prev_url in previous_urls:
                    batch.append((card_id, card_previous_evolve, prev_url, 0))  # card_poke_finale par défaut à 0

                # Insérer par batch
                if len(batch) >= BATCH_SIZE:
                    try:
                        cur.executemany("""
                            INSERT INTO card_evolve (
                                card_id, card_previous_evolve, card_previous_url, card_poke_finale
                            ) VALUES (%s, %s, %s, %s);
                        """, batch)
                        conn.commit()
                        total_inserted += len(batch)
                        print(f"✅ Insertion de {len(batch)} lignes (total: {total_inserted})")
                        batch.clear()
                    except Exception as e:
                        print(f"⚠️ Erreur d'insertion batch : {e}")
                        conn.rollback()
                        batch.clear()

            # Insertion finale
            if batch:
                try:
                    cur.executemany("""
                        INSERT INTO card_evolve (
                            card_id, card_previous_evolve, card_previous_url, card_poke_finale
                        ) VALUES (%s, %s, %s, %s);
                    """, batch)
                    conn.commit()
                    total_inserted += len(batch)
                    print(f"✅ Insertion finale de {len(batch)} lignes (total: {total_inserted})")
                except Exception as e:
                    print(f"⚠️ Erreur d'insertion finale : {e}")
                    conn.rollback()

        cur.close()

        # Étape 3 : Mise à jour optimisée de card_poke_finale
        print("\n🎯 Mise à jour card_poke_finale...")
        update_card_poke_finale_optimized(conn)

        # Étape 4 : Finalisation avec index
        print("📊 Création des index...")
        cur = conn.cursor()
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_card_evolve_card_id ON card_evolve(card_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_card_evolve_finale ON card_evolve(card_poke_finale);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_card_evolve_previous_url ON card_evolve(card_previous_url);")
            conn.commit()
            print("✅ Index créés")
        except Exception as e:
            print(f"⚠️ Erreur création index : {e}")
        finally:
            cur.close()

    except Exception as e:
        print(f"💥 Erreur principale : {e}")
        conn.rollback()
    finally:
        conn.close()
        print("🔌 Connexion PostgreSQL fermée.")

if __name__ == '__main__':
    start_time = time.time()
    update_card_evolve()
    print(f"⏱️ Terminé en {round(time.time() - start_time, 2)}s")
