"""View Creation for Reporting

This script specified views in a PostgreSQL database.

This file contains the following
functions:

    * create_view - creates a view in a PostgreSQL database
    * main - the main function of the script
"""

import psycopg2
import pandas as pd
import datetime as dt
import os

def create_view(view):
    """Creates a view in PostgreSQL database

    Parameters
    ----------
    view : str
        The SQL query to create the view
    """
    connection = None
    try:
        # connect to the PostgreSQL server
        connection = psycopg2.connect(
            user='postgres',
            database="9dt_db",
            password='1234',
            host='localhost',
            port=5432
            )
        connection.set_client_encoding("utf-8")
        cursor = connection.cursor()
        # create view
        cursor.execute(view)
        cursor.close()
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

if __name__ == '__main__':
    create_view("CREATE OR REPLACE VIEW rank_win_first_move AS SELECT column_pos, cnt frequency, ROUND(cast(cnt as decimal)/(SELECT COUNT(1) FROM game_results WHERE result='win')*100,2) percent FROM (SELECT column_pos, COUNT(1) cnt FROM game_results INNER JOIN game_moves ON game_results.game_id = game_moves.game_id AND game_results.player_id = game_results.player_id WHERE result = 'win' AND move_number = 1 GROUP BY column_pos) a ORDER BY cnt DESC;")
    create_view("CREATE OR REPLACE VIEW nationality_games AS SELECT nat, COUNT(1) num_games FROM (SELECT DISTINCT game_results.game_id, player_data.nat FROM game_results INNER JOIN player_data ON player_data.id = game_results.player_id) a GROUP BY nat;")
    create_view("CREATE OR REPLACE VIEW marketing_one_game_email AS SELECT player_id, email, result FROM game_results JOIN player_data ON game_results.player_id = player_data.id WHERE player_id IN (SELECT player_id FROM game_results GROUP BY player_id HAVING COUNT(1) = 1);")
