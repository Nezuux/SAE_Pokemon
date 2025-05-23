import psycopg
import os
import json
from datetime import datetime

postgres_db=os.environ.get('POSTGRES_DB')
postgres_user=os.environ.get('POSTGRES_USER')
postgres_password=os.environ.get('POSTGRES_PASSWORD')
postgres_host=os.environ.get('POSTGRES_HOST')
postgres_port=os.environ.get('POSTGRES_PORT')

output_directory = "../data_collection/sample_output"

def get_connection_string():
  return f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"

def execute_sql_script(path: str):
  with psycopg.connect(get_connection_string()) as conn:
    with conn.cursor() as cur:
      with open(path) as f:
        cur.execute(f.read())

def insert_wrk_tournaments():
  tournament_data = []
  for file in os.listdir(output_directory):
    with open(f"{output_directory}/{file}") as f:
      tournament = json.load(f)
      tournament_data.append((
        tournament['id'], 
        tournament['name'], 
        datetime.strptime(tournament['date'], '%Y-%m-%dT%H:%M:%S.000Z'),
        tournament['organizer'], 
        tournament['format'], 
        int(tournament['nb_players'])
        ))
  
  with psycopg.connect(get_connection_string()) as conn:
    with conn.cursor() as cur:
      cur.executemany("INSERT INTO public.wrk_tournaments values (%s, %s, %s, %s, %s, %s)", tournament_data)

def insert_wrk_decklists():
  decklist_data = []
  for file in os.listdir(output_directory):
    with open(f"{output_directory}/{file}") as f:
      tournament = json.load(f)
      tournament_id = tournament['id']
      for player in tournament['players']:
        player_id = player['id']
        for card in player['decklist']:
          decklist_data.append((
            tournament_id,
            player_id,
            card['type'],
            card['name'],
            card['url'],
            int(card['count']),
          ))
  
  with psycopg.connect(get_connection_string()) as conn:
    with conn.cursor() as cur:
      cur.executemany("INSERT INTO public.wrk_decklists values (%s, %s, %s, %s, %s, %s)", decklist_data)

print("creating work tables")
execute_sql_script("00_create_wrk_tables.sql")

print("insert raw tournament data")
insert_wrk_tournaments()

print("insert raw decklist data")
insert_wrk_decklists()

print("construct card database")
execute_sql_script("01_dwh_cards.sql")
