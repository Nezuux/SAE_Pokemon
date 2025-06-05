# -*- coding: utf-8 -*-
import sys
import os

# Configuration de l'encodage en UTF-8 pour stdout et stderr
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import psycopg2
import requests
from bs4 import BeautifulSoup
import time

# Nettoyage de texte (espaces, sauts de ligne)
def clean(text):
    return text.strip().replace('\n', '').replace('  ', ' ')

# Supprime les caractères non-ASCII (ex. : pour les champs texte simples)
def remove_non_utf8(text):
    return text.encode('ascii', 'ignore').decode('ascii').strip() if text else None

# Associe un nom d'élément à son URL d'image
def get_element_url(element):
    element_mapping = {
        'colorless': 'https://pokexp.com/uploads/2021/07/08072021-normal.png',
        'darkness': 'https://pokexp.com/uploads/2021/07/08072021-tnbre.png',
        'dragon': 'https://pokexp.com/uploads/2021/07/08072021-dragon.png',
        'fighting': 'https://pokexp.com/uploads/2021/07/08072021-combat.png',
        'fire': 'https://pokexp.com/uploads/2021/07/08072021-feu.png',
        'grass': 'https://pokexp.com/uploads/2021/07/08072021-plante.png',
        'lightning': 'https://pokexp.com/uploads/2021/07/08072021-lumire.png',
        'metal': 'https://pokexp.com/uploads/2021/07/08072021-acier.png',
        'psychic': 'https://pokexp.com/uploads/2021/07/08072021-psy.png',
        'water': 'https://pokexp.com/uploads/2021/07/08072021-eau.png'
    }
    corrections = {
        'figthing': 'fighting', 'ligthning': 'lightning'
    }
    element_clean = corrections.get(element.lower().strip(), element.lower().strip())
    return element_mapping.get(element_clean)

# Extrait les informations détaillées d'une carte depuis son URL
def extraire_infos_depuis_page(url, card_type):
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')

        # Extraction uniquement pour les cartes de type Pokémon
        element, hp = None, None
        if card_type == "Pok mon":
            titre = soup.find('p', class_='card-text-title')
            if titre:
                for part in titre.get_text(separator=' ', strip=True).split(' - '):
                    if 'HP' in part:
                        hp = part.replace('HP', '').strip()
                    elif part and not part.endswith('ex') and part != titre:
                        element = part.strip()

        weakness = retreat = None
        for block in soup.find_all('p', class_='card-text-wrr'):
            txt = block.get_text(separator=' ', strip=True)
            if 'Weakness:' in txt and 'Retreat:' in txt:
                parts = txt.split('Retreat:')
                weakness = parts[0].replace('Weakness:', '').strip()
                retreat = parts[1].strip()

        version = version_code = None
        bloc = soup.find('div', class_='card-prints-current')
        if bloc:
            version = bloc.find('span', class_='text-lg')
            version = version.get_text(strip=True) if version else None
            img = bloc.find('img', class_='set')
            version_code = img['alt'] if img and img.has_attr('alt') else None

        evolution_from = None
        type_section = soup.find('p', class_='card-text-type')
        if type_section and 'Evolves from' in type_section.text:
            link = type_section.find('a')
            if link:
                evolution_from = link.get_text(strip=True)

        image_url = None
        img_div = soup.find('div', class_='card-image')
        if img_div:
            img_tag = img_div.find('img')
            image_url = img_tag['src'] if img_tag and img_tag.has_attr('src') else None

        return (
            element,
            int(hp) if hp and hp.isdigit() else None,
            weakness,
            retreat,
            version,
            version_code,
            evolution_from,
            image_url
        )
    except Exception as e:
        print(f"⚠️ Erreur sur {url} : {e}")
        return (None,) * 8

# Crée une connexion PostgreSQL en UTF-8
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
        return psycopg2.connect(
            host='localhost', 
            port=5432, 
            dbname='postgres', 
            user='postgres'
        )

# Crée ou recrée la table card_complement
def create_card_complement_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DROP TABLE IF EXISTS card_complement CASCADE;
                CREATE TABLE card_complement (
                    card_id TEXT PRIMARY KEY,
                    card_element TEXT,
                    card_hp INTEGER,
                    card_weakness TEXT,
                    card_retreat TEXT,
                    card_extension_name TEXT,
                    extension_code TEXT,
                    card_previous_evolve TEXT,
                    card_image_url TEXT,
                    card_element_url TEXT,
                    FOREIGN KEY (card_id) REFERENCES card(card_id)
                );
            """)
        conn.commit()
        print("🧱 Table card_complement créée.")

# Met à jour les données complémentaires manquantes pour les cartes
def maj_cartes():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.card_id, c.card_type FROM card c
                WHERE c.card_id NOT IN (SELECT card_id FROM card_complement);
            """)
            cartes = cur.fetchall()

            print(f"🔄 {len(cartes)} cartes à compléter...")
            updated = 0

            for card_id, card_type in cartes:
                infos = extraire_infos_depuis_page(card_id, card_type)
                if any(infos):
                    infos_cleaned = tuple(remove_non_utf8(x) if isinstance(x, str) else x for x in infos)
                    try:
                        cur.execute("""
                            INSERT INTO card_complement (
                                card_id, card_element, card_hp, card_weakness, card_retreat,
                                card_extension_name, extension_code, card_previous_evolve, card_image_url
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (card_id) DO UPDATE SET
                                card_element = EXCLUDED.card_element,
                                card_hp = EXCLUDED.card_hp,
                                card_weakness = EXCLUDED.card_weakness,
                                card_retreat = EXCLUDED.card_retreat,
                                card_extension_name = EXCLUDED.card_extension_name,
                                extension_code = EXCLUDED.extension_code,
                                card_previous_evolve = EXCLUDED.card_previous_evolve,
                                card_image_url = EXCLUDED.card_image_url;
                        """, (card_id, *infos_cleaned))
                        updated += 1
                        type_info = f" (Type: {card_type})" if card_type == "Pok mon" else f" (Type: {card_type} - pas d'élément)"
                        print(f"✅ {card_id} mis à jour{type_info}")
                    except Exception as e:
                        print(f"⚠️ Erreur pour {card_id} : {e}")
                time.sleep(0.1)

            conn.commit()
            print(f"\n✅ Mise à jour terminée ({updated} cartes modifiées).")

# Met à jour l'URL de l'élément (élément → image) pour les cartes de type Pokémon
def update_element_urls():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cc.card_id, cc.card_element 
                FROM card_complement cc
                JOIN card c ON cc.card_id = c.card_id
                WHERE cc.card_element IS NOT NULL 
                AND cc.card_element_url IS NULL
                AND c.card_type = 'Pok mon';
            """)
            cartes = cur.fetchall()

            print(f"🔄 Mise à jour des card_element_url pour {len(cartes)} cartes Pokémon...")
            updated = 0

            for card_id, element in cartes:
                print(f"🔎 ID={card_id}, Élément='{element}'")
                element_url = get_element_url(element)
                if element_url:
                    try:
                        cur.execute("""
                            UPDATE card_complement
                            SET card_element_url = %s
                            WHERE card_id = %s;
                        """, (element_url, card_id))
                        updated += 1
                        print(f"✅ {card_id} : {element} → {element_url}")
                    except Exception as e:
                        print(f"⚠️ Erreur pour {card_id} : {e}")
                else:
                    print(f"❌ Élément inconnu : '{element}' (ID {card_id})")

            conn.commit()
            print(f"✅ Mise à jour des card_element_url terminée ({updated} cartes Pokémon modifiées).")

# Point d'entrée principal
if __name__ == '__main__':
    print("🧱 Création de la table card_complement...")
    create_card_complement_table()

    print("\n📦 Mise à jour des infos complémentaires...")
    maj_cartes()

    print("\n" + "="*50)
    print("🎯 MISE À JOUR DES CARD_ELEMENT_URL")
    print("="*50)
    update_element_urls()
