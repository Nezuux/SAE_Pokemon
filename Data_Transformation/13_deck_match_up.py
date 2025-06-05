import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("deck_counters_insertion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Connexion à PostgreSQL
engine = create_engine(
    "postgresql+psycopg2://postgres:@localhost:5432/postgres"
)

# Suppression de la table si elle existe
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS deck_counters;"))
    conn.commit()
logger.info("Ancienne table deck_counters supprimée (si existante).")

# Création de la nouvelle table avec toutes les colonnes
create_table_sql = """
CREATE TABLE IF NOT EXISTS deck_counters (
    deck_name TEXT PRIMARY KEY,
    best_counter TEXT,
    best_counter_winrate FLOAT,
    nb_match INTEGER,
    nb_victoire INTEGER,
    nb_defaites INTEGER
);
"""
with engine.connect() as conn:
    conn.execute(text(create_table_sql))
    conn.commit()
logger.info("Table deck_counters créée.")

# Lecture des données depuis match_winners_losers
df = pd.read_sql("""
    SELECT winner_deck_name, looser_deck_name
    FROM match_winners_losers
    WHERE winner_deck_name IS NOT NULL
      AND looser_deck_name IS NOT NULL
      AND winner_deck_name <> looser_deck_name
""", engine)

# Nombre de victoires pour chaque matchup (counter gagne contre deck_name)
wins = df.groupby(['winner_deck_name', 'looser_deck_name']).size().reset_index(name='wins')

# Nombre de défaites pour chaque matchup (counter perd contre deck_name)
losses = df.groupby(['looser_deck_name', 'winner_deck_name']).size().reset_index(name='losses')

# Nombre total de matchs entre chaque paire de decks (quel que soit le sens)
total_matches_1 = df.groupby(['winner_deck_name', 'looser_deck_name']).size().reset_index(name='count1')
total_matches_2 = df.groupby(['looser_deck_name', 'winner_deck_name']).size().reset_index(name='count2')

# Fusion pour obtenir le total de matchs dans les deux sens
merged_counts = pd.merge(
    total_matches_1,
    total_matches_2,
    left_on=['winner_deck_name', 'looser_deck_name'],
    right_on=['looser_deck_name', 'winner_deck_name'],
    how='outer'
).fillna(0)

# Renommer les colonnes pour plus de clarté
merged_counts = merged_counts.rename(columns={
    'winner_deck_name_x': 'winner_deck_name',
    'looser_deck_name_x': 'looser_deck_name',
    'count1': 'count1',
    'count2': 'count2'
})

# Calcul du total des matchs entre les deux decks
merged_counts['total_matches'] = merged_counts['count1'] + merged_counts['count2']

# Préparation du DataFrame des victoires et défaites
wins = wins.rename(columns={'winner_deck_name': 'counter', 'looser_deck_name': 'deck_name'})
losses = losses.rename(columns={'looser_deck_name': 'counter', 'winner_deck_name': 'deck_name'})

# Fusion des victoires, défaites, et total des matchs
matchups = pd.merge(
    wins,
    losses,
    on=['counter', 'deck_name'],
    how='outer'
).fillna(0)

matchups = pd.merge(
    matchups,
    merged_counts[["winner_deck_name", "looser_deck_name", "total_matches"]],
    left_on=['counter', 'deck_name'],
    right_on=['winner_deck_name', 'looser_deck_name'],
    how='left'
).fillna(0)

matchups['wins'] = matchups['wins'].astype(int)
matchups['losses'] = matchups['losses'].astype(int)
matchups['total_matches'] = matchups['total_matches'].astype(int)

# Laplace smoothing
k = 5  # paramètre de lissage, ajustable
matchups['winrate_lisse'] = (matchups['wins'] + k) / (matchups['total_matches'] + 2 * k)

# On peut aussi filtrer les matchups avec peu de matchs
min_match = 5  # seuil minimal de matchs pour qu'un matchup soit pris en compte
matchups = matchups[matchups['total_matches'] >= min_match]

# Pour chaque deck, trouver le meilleur counter selon le winrate lissé
best_counters = (
    matchups.sort_values(['deck_name', 'winrate_lisse', 'total_matches'], ascending=[True, False, False])
    .groupby('deck_name')
    .first()
    .reset_index()
    .rename(columns={
        'counter': 'best_counter',
        'winrate_lisse': 'best_counter_winrate',
        'total_matches': 'nb_match',
        'wins': 'nb_victoire',
        'losses': 'nb_defaites'
    })
    [['deck_name', 'best_counter', 'best_counter_winrate', 'nb_match', 'nb_victoire', 'nb_defaites']]
)

# Insertion dans la table PostgreSQL
best_counters.to_sql('deck_counters', engine, if_exists='append', index=False, method='multi')
logger.info("Table deck_counters remplie avec succès.")

print("Aperçu du résultat :")
print(best_counters.head())

logger.info("Insertion terminée.")
print("Insertion terminée.")
