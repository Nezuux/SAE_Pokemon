import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("insertion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Connexion à la base PostgreSQL
engine = create_engine(
    "postgresql+psycopg2://postgres:@localhost:5432/postgres"
)

# Suppression de la table si elle existe
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS match_winners_losers;"))
    conn.commit()
logger.info("Ancienne table match_winners_losers supprimée (si existante).")

# Création de la table cible avec IDs joueurs en TEXT
create_table_sql = """
CREATE TABLE IF NOT EXISTS match_winners_losers (
    match_id INTEGER PRIMARY KEY,
    winner_id TEXT,
    looser_id TEXT,
    winner_deck_name TEXT,
    looser_deck_name TEXT
);
"""

with engine.connect() as conn:
    conn.execute(text(create_table_sql))
    conn.commit()
logger.info("Table match_winners_losers créée.")

# Extraction des données
query = """
SELECT
    m.match_id,
    m.match_winner AS winner_id,
    dm2.player_id AS looser_id,
    d1.deck_nom AS winner_deck_name,
    d2.deck_nom AS looser_deck_name
FROM
    match m
JOIN deck_match dm1 ON dm1.match_id = m.match_id AND dm1.player_id = m.match_winner
JOIN deck d1 ON d1.deck_id = dm1.deck_id
JOIN deck_match dm2 ON dm2.match_id = m.match_id AND dm2.player_id != m.match_winner
JOIN deck d2 ON d2.deck_id = dm2.deck_id
;
"""

df = pd.read_sql(query, engine)
logger.info(f"{len(df)} lignes extraites pour insertion.")

# Insertion en batch avec pandas (bien plus rapide)
df.to_sql('match_winners_losers', engine, if_exists='append', index=False, method='multi')
logger.info("Toutes les lignes insérées en une seule opération.")

# Affiche un aperçu des premières lignes insérées
print("Aperçu des premières lignes insérées :")
print(df.head())

logger.info("Insertion terminée.")
print("Insertion terminée.")
