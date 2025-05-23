import psycopg
import os
import json
from datetime import datetime


output_directory = "../data_collection/tournament"
output_directory = os.path.join(os.path.dirname(__file__), output_directory)

def get_connection():
    return psycopg. connect(
        host="localhost",
        port="5432",
        dbname="postgres",
        user="postgres",
        password=""
    )

def execute_sql_script(path: str):
    full_path = os.path.join(os.path.dirname(__file__), path)  # 🔍 construit le chemin absolu
    with get_connection() as conn:
        with conn.cursor() as cur:
            with open(full_path) as f:
                cur.execute(f.read())
        conn.commit()       

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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO public.wrk_tournaments values (%s, %s, %s, %s, %s, %s)",
                tournament_data
            )
        conn.commit()

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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO public.wrk_decklists values (%s, %s, %s, %s, %s, %s)",
                decklist_data
            )
        conn.commit()

print("creating work tables")
execute_sql_script("00_create_wrk_tables.sql")

print("insert raw tournament data")
insert_wrk_tournaments()

print("insert raw decklist data")
insert_wrk_decklists()

print("construct card database")
execute_sql_script("01_dwh_cards.sql")


