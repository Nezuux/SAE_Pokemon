# -*- coding: utf-8 -*-
import sys
import os

# ⚙️ Forcer l'encodage UTF-8 pour la sortie standard et les erreurs
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# 📦 Import des bibliothèques nécessaires
import psycopg2  # Connexion PostgreSQL
import requests  # Requêtes HTTP
from bs4 import BeautifulSoup  # Parsing HTML
from concurrent.futures import ThreadPoolExecutor, as_completed  # Multithreading
from urllib.parse import urljoin  # Construction d'URL absolue
import re  # Expressions régulières
import time  # Mesure du temps

BATCH_SIZE = 50  # Nombre d'enregistrements à insérer par lot

# 🧹 Nettoyage de texte (suppression des caractères non-ASCII)
def clean_text(text):
    if not text:
        return None
    return re.sub(r'[^\x00-\x7F]+', ' ', text).strip()

# 🔎 Récupère le nom de l’évolution précédente depuis une page de carte
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

# 🔗 Récupère toutes les URLs des évolutions précédentes depuis la page d'une carte
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

# 🔌 Connexion PostgreSQL avec gestion UTF-8 robuste
def get_conn():
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
        # Connexion de secours en cas d'erreur d'encodage
        return psycopg2.connect(
            host='localhost', 
            port=5432, 
            dbname='postgres', 
            user='postgres'
        )

# 🛠️ Met à jour la colonne 'card_poke_finale' (1 = finale, 0 = évolution)
def update_card_poke_finale_optimized(conn):
    cur = conn.cursor()
    try:
        print("🔍 Vérification ou création de la colonne 'card_poke_finale'...")

        # Vérifie si la colonne existe, sinon la crée ou la renomme
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='card_evolve' AND column_name='card_poke_finale';")
        if not cur.fetchone():
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='card_evolve' AND column_name='next';")
            if cur.fetchone():
                cur.execute("ALTER TABLE card_evolve RENAME COLUMN next TO card_poke_finale;")
                print("✅ Colonne 'next' renommée en 'card_poke_finale'")
            else:
                cur.execute("ALTER TABLE card_evolve ADD COLUMN card_poke_finale INT DEFAULT 0;")
                print("✅ Colonne 'card_poke_finale' créée")
            conn.commit()

        print("📥 Mise à jour card_poke_finale avec SQL optimisé...")

        # Si la carte est une évolution précédente, alors ce n’est pas une carte finale
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
        conn.commit()
        print(f"✅ card_poke_finale mis à jour pour {cur.rowcount} lignes")

    except Exception as e:
        print(f"💥 Erreur mise à jour card_poke_finale : {e}")
        conn.rollback()
    finally:
        cur.close()

# 🚀 Fonction principale pour scraper et alimenter la table card_evolve
def update_card_evolve():
    conn = get_conn()
    cur = conn.cursor()

    try:
        # 📦 Création ou réinitialisation de la table
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

        # 🎯 Sélection des cartes Pokémon à traiter
        cur.execute("SELECT card_id FROM card WHERE card_id IS NOT NULL AND card_type = 'Pok mon';")
        urls = [url for (url,) in cur.fetchall()]
        print(f"🔗 {len(urls)} cartes Pokémon à traiter...")

        if not urls:
            print("⚠️ Aucune carte Pokémon trouvée")
            return

        # Étape 1 : Récupérer les noms des évolutions précédentes
        print("🔍 Scraping card_previous_evolve...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_evolution_from, url) for url in urls]
            evo_from_results = {}
            for future in as_completed(futures):
                url, evo_from = future.result()
                evo_from_results[url] = evo_from

        # Étape 2 : Récupérer les URLs d’évolutions précédentes + insertion par lots
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
                    batch.append((card_id, card_previous_evolve, prev_url, 0))  # valeur par défaut

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

            # Dernier batch restant
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

        # Étape 3 : Mise à jour finale via SQL
        print("\n🎯 Mise à jour card_poke_finale...")
        update_card_poke_finale_optimized(conn)

        # Étape 4 : Indexation pour optimisation des requêtes futures
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

# ▶️ Exécution du script
if __name__ == '__main__':
    start_time = time.time()
    update_card_evolve()
    print(f"⏱️ Terminé en {round(time.time() - start_time, 2)}s")
