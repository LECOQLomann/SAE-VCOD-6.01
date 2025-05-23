
import oracledb
import os
import json
from datetime import datetime

oracle_user = "E2101316"
oracle_password = "24Fevrier03"
oracle_host = "ora23ai.univ-ubs.fr"
oracle_port = "1521"
oracle_service_name = "ORAETUD"

output_directory = "../data_collection/tournament"

def get_connection():
    dsn = f"{oracle_host}:{oracle_port}/{oracle_service_name}"  
    connection = oracledb.connect(user=oracle_user, password=oracle_password, dsn=dsn, retry_count=3)
    return connection

def execute_sql_script(path: str):
    with get_connection() as conn:
        with conn.cursor() as cur:
            with open(path) as f:
                sql = f.read()
                cur.execute(sql)
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
            sql = """
                INSERT INTO wrk_tournaments (id, name, date, organizer, format, nb_players)
                VALUES (:1, :2, :3, :4, :5, :6)
            """
            cur.executemany(sql, tournament_data)
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
            sql = """
                INSERT INTO wrk_decklists (tournament_id, player_id, type, name, url, count)
                VALUES (:1, :2, :3, :4, :5, :6)
            """
            cur.executemany(sql, decklist_data)
        conn.commit()

print("creating work tables")
execute_sql_script("00_create_wrk_tables.sql")

print("insert raw tournament data")
insert_wrk_tournaments()

print("insert raw decklist data")
insert_wrk_decklists()

print("construct card database")
execute_sql_script("01_dwh_cards.sql")








# import cx_Oracle
# import os
# import json
# from datetime import datetime

# oracle_user = "E2101316"
# oracle_password = "24Fevrier03"
# oracle_host = "ora23ai.univ-ubs.fr"
# oracle_port = "1521"
# oracle_service_name = "ORAETUD"

# output_directory = "../data_collection/tournament"

# def get_connection():
#     dsn_tns = cx_Oracle.makedsn(oracle_host, oracle_port, sid=oracle_sid)
#     connection = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn_tns)
#     return connection

# def execute_sql_script(path: str):
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             with open(path) as f:
#                 sql = f.read()
#                 cur.execute(sql)
#         conn.commit()

# def insert_wrk_tournaments():
#     tournament_data = []
#     for file in os.listdir(output_directory):
#         with open(f"{output_directory}/{file}") as f:
#             tournament = json.load(f)
#             tournament_data.append((
#                 tournament['id'],
#                 tournament['name'],
#                 datetime.strptime(tournament['date'], '%Y-%m-%dT%H:%M:%S.000Z'),
#                 tournament['organizer'],
#                 tournament['format'],
#                 int(tournament['nb_players'])
#             ))
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             sql = """
#                 INSERT INTO wrk_tournaments (id, name, date, organizer, format, nb_players)
#                 VALUES (:1, :2, :3, :4, :5, :6)
#             """
#             cur.executemany(sql, tournament_data)
#         conn.commit()

# def insert_wrk_decklists():
#     decklist_data = []
#     for file in os.listdir(output_directory):
#         with open(f"{output_directory}/{file}") as f:
#             tournament = json.load(f)
#             tournament_id = tournament['id']
#             for player in tournament['players']:
#                 player_id = player['id']
#                 for card in player['decklist']:
#                     decklist_data.append((
#                         tournament_id,
#                         player_id,
#                         card['type'],
#                         card['name'],
#                         card['url'],
#                         int(card['count']),
#                     ))
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             sql = """
#                 INSERT INTO wrk_decklists (tournament_id, player_id, type, name, url, count)
#                 VALUES (:1, :2, :3, :4, :5, :6)
#             """
#             cur.executemany(sql, decklist_data)
#         conn.commit()

# print("creating work tables")
# execute_sql_script("00_create_wrk_tables.sql")

# print("insert raw tournament data")
# insert_wrk_tournaments()

# print("insert raw decklist data")
# insert_wrk_decklists()

# print("construct card database")
# execute_sql_script("01_dwh_cards.sql")
