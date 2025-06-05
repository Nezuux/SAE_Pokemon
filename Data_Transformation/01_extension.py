import psycopg2
import pandas as pd
import os

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
    print("Table 'extension' créée.")

def insert_extensions_data(df):
    conn = psycopg2.connect(host='localhost', port=5432, dbname='postgres', user='postgres')
    cur = conn.cursor()

    inserted = 0
    for _, row in df.iterrows():
        try:
            extension_code = row["Code_extension"]
            extension_name = row['Extension']
            extension_nb_card = int(row['Nb_carte'])
            extension_date_sortie = pd.to_datetime(row['Date'], dayfirst=True).date()

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
            code = row.get("Code_extension", 'inconnu')
            print(f"⚠️ Erreur insertion pour {code} : {e}")

    conn.commit()
    conn.close()
    print(f"{inserted} lignes insérées/mises à jour dans 'extension'.")

if __name__ == '__main__':
    path_excel = os.getenv("PATH_EXCEL")
    if not path_excel:
        raise ValueError("La variable d'environnement PATH_EXCEL n'est pas définie.")
    df_extensions = pd.read_excel(path_excel)

    print(f"Colonnes Excel importées : {list(df_extensions.columns)}")

    create_extension_table()
    insert_extensions_data(df_extensions)
