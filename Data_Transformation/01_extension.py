# Import des bibliothèques nécessaires
import psycopg2  # Pour se connecter et interagir avec une base de données PostgreSQL
import pandas as pd  # Pour manipuler des données tabulaires (DataFrame)
import os  # Pour accéder aux variables d'environnement

# Fonction pour créer la table 'extension' dans la base de données
def create_extension_table():
    # Connexion à la base de données PostgreSQL
    conn = psycopg2.connect(host='localhost', port=5432, dbname='postgres', user='postgres')
    cur = conn.cursor()

    # Suppression de la table 'extension' si elle existe déjà, puis création d'une nouvelle table
    cur.execute("""
        DROP TABLE IF EXISTS extension;
        CREATE TABLE extension (
            extension_code TEXT PRIMARY KEY,                -- Code unique de l'extension
            extension_name TEXT,                            -- Nom de l'extension
            extension_nb_card INTEGER,                      -- Nombre de cartes dans l'extension
            extension_date_sortie DATE                      -- Date de sortie de l'extension
        );
    """)

    # Validation des modifications et fermeture de la connexion
    conn.commit()
    conn.close()
    print("Table 'extension' créée.")

# Fonction pour insérer les données du DataFrame dans la table 'extension'
def insert_extensions_data(df):
    # Connexion à la base de données
    conn = psycopg2.connect(host='localhost', port=5432, dbname='postgres', user='postgres')
    cur = conn.cursor()

    inserted = 0  # Compteur de lignes insérées ou mises à jour

    # Parcours de chaque ligne du DataFrame
    for _, row in df.iterrows():
        try:
            # Récupération et transformation des données de la ligne
            extension_code = row["Code_extension"]
            extension_name = row['Extension']
            extension_nb_card = int(row['Nb_carte'])
            extension_date_sortie = pd.to_datetime(row['Date'], dayfirst=True).date()

            # Insertion dans la table avec mise à jour en cas de conflit sur la clé primaire
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
            inserted += 1  # Incrémentation du compteur si l'insertion/mise à jour réussit

        except Exception as e:
            # Gestion des erreurs : affichage du code d'extension problématique
            code = row.get("Code_extension", 'inconnu')
            print(f"⚠️ Erreur insertion pour {code} : {e}")

    # Validation des modifications et fermeture de la connexion
    conn.commit()
    conn.close()
    print(f"{inserted} lignes insérées/mises à jour dans 'extension'.")

# Bloc principal exécuté si le script est lancé directement
if __name__ == '__main__':
    # Récupération du chemin vers le fichier Excel via une variable d'environnement
    path_excel = os.getenv("PATH_EXCEL")
    if not path_excel:
        raise ValueError("La variable d'environnement PATH_EXCEL n'est pas définie.")

    # Lecture du fichier Excel dans un DataFrame pandas
    df_extensions = pd.read_excel(path_excel)

    # Affichage des noms de colonnes pour vérification
    print(f"Colonnes Excel importées : {list(df_extensions.columns)}")

    # Création de la table puis insertion des données
    create_extension_table()
    insert_extensions_data(df_extensions)
