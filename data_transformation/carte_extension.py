import psycopg2
import pandas as pd

def create_extension_table():
    conn = psycopg2.connect(host='localhost', port=5432, dbname='postgres', user='postgres')
    cur = conn.cursor()
    cur.execute("""
        DROP TABLE IF EXISTS extension;
        CREATE TABLE extension (
            extension_code TEXT PRIMARY KEY,
            extension_name TEXT,
            extension_nb_card INTEGER,
            extension_date_sortie DATE
        );
    """)
    conn.commit()
    conn.close()
    print("✅ Table 'extension' créée.")

def insert_extensions_data(df):
    conn = psycopg2.connect(host='localhost', port=5432, dbname='postgres', user='postgres')
    cur = conn.cursor()

    inserted = 0
    for _, row in df.iterrows():
        try:
            extension_code = row["Numéro d'extension"]
            extension_name = row['Extension']
            extension_nb_card = int(row['Nombre de cartes '])  # attention à l'espace final
            extension_date_sortie = pd.to_datetime(row['Date de sortie'], dayfirst=True).date()

            cur.execute("""
                INSERT INTO extension (extension_code, extension_name, extension_nb_card, extension_date_sortie)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (extension_code) DO UPDATE SET
                    extension_name = EXCLUDED.extension_name,
                    extension_nb_card = EXCLUDED.extension_nb_card,
                    extension_date_sortie = EXCLUDED.extension_date_sortie;
            """, (
                extension_code,
                extension_name,
                extension_nb_card,
                extension_date_sortie
            ))
            inserted += 1
        except Exception as e:
            print(f"⚠️ Erreur insertion pour {row.get('Numéro d\'extension', 'inconnu')} : {e}")

    conn.commit()
    conn.close()
    print(f"✅ {inserted} lignes insérées/mises à jour dans 'extension'.")

if __name__ == '__main__':
    path_excel = r"E:\DataCollection\Table correpondance Extension pokémon.xlsx"
    df_extensions = pd.read_excel(path_excel)

    print(f"Colonnes Excel importées : {list(df_extensions.columns)}")

    create_extension_table()
    insert_extensions_data(df_extensions)
