"""Game Data ETL

This script processes 9dt game data from a csv into a PostgreSQL database.

This file contains the following functions:

    * create_tables - creates tables in a PostgreSQL database
    * transform_game_data - returns a DataFrame of cleaned game data
    * transform_game_moves - returns a DataFrame of cleaned game moves data
    * tranform_game_results = returns a DataFrame of cleaned game results data
    * load_data - loads data from dataframe into specified table in PostgreSQL
    * main - the main function of the script
"""

import psycopg2
import pandas as pd
import datetime as dt
import os


def create_tables():
    """Creates tables in PostgreSQL database"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS game_results (
            id SERIAL,
            game_id INT NOT NULL,
            player_id INT NOT NULL,
            result VARCHAR(25),
            insert_dtm timestamp NOT NULL DEFAULT NOW(),
            PRIMARY KEY(game_id, player_id),
            FOREIGN KEY (player_id) REFERENCES player_data (id)
        )""",
        """
        CREATE TABLE IF NOT EXISTS game_moves (
            id SERIAL PRIMARY KEY,
            game_id INT NOT NULL,
            player_id INT NOT NULL,
            move_number INT NOT NULL,
            column_pos INT NOT NULL,
            insert_dtm timestamp NOT NULL DEFAULT NOW(),
            FOREIGN KEY (game_id, player_id) REFERENCES GAME_RESULTS (game_id, player_id)
        )"""
        )
    connection = None
    try:
        connection = psycopg2.connect(
            user='postgres',
            database="9dt_db",
            password='1234',
            host='localhost',
            port=5432
            )
        cursor = connection.cursor()
        # loops through and creates tables
        for command in commands:
            cursor.execute(command)
        cursor.close()
        connection.commit()
        print("Tables created.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

def transform_game_data(file_loc):
    """Loads and cleans game data from a CSV.

    Parameters
    ----------
    file_loc : str
        The file location of the spreadsheet

    Returns
    -------
    games
        a Pandas DataFrame of cleaned game data
    """
    games = pd.read_csv(file_loc)

    # interpolate erroneous game_ids
    previous = ''
    tmp = ''
    for index, row in games.iterrows():
        if row.game_id.isnumeric():
            pass
        else:
            tmp = row.game_id
            i = index
            while games.loc[i,'game_id'] == tmp:
                games.loc[i,'game_id'] = int(previous) + 1
                i += 1
        previous = row.game_id
        tmp = ''
    return games

def transform_game_moves(games):
    """Processes game moves data

    Parameters
    ----------
    games : DataFrame
        Pandas DataFrame of processed game data

    Returns
    -------
    game_moves
        a Pandas Dataframe of transformed game moves data
    """
    game_moves = games.drop(columns=['result'])
    game_moves = game_moves.rename(columns={'column': 'column_pos'})
    return game_moves

def tranform_game_results(games):
    """Processes game results data

    Parameters
    ----------
    games : DataFrame
        Pandas Dataframe of processed game data

    Returns
    -------
    game_results
        a Pandas DataFrame of transformed game results data
    """

    # create df of all player/game combination in initial games data
    game_results = games.filter(['game_id','player_id']).drop_duplicates()
    # update with game results where it exists in the moves data
    game_results = game_results.merge(games.dropna(subset=['result']),how='outer',on=['game_id','player_id'])
    game_results = game_results.filter(['game_id','player_id','result'])

    # get game id where result = draw and update other to draw
    game_draw = game_results[game_results['result']=='draw'].game_id
    game_results.loc[game_results.game_id.isin(game_draw), 'result'] = 'draw'
    # get game id where result = win and update other to lose
    game_win = game_results[game_results['result']=='win'].game_id
    game_results.loc[(game_results.game_id.isin(game_win)) & (game_results['result'].isna()) , 'result'] = 'lose'

    return game_results

def load_data(df, table):
    """Loads provided DataFrame to specified table in PostgreSQL database.

    Parameters
    ----------
    df : DataFrame
        Pandas DataFrame of processed data
    table : str
        Name of the PostgreSQL table the data will be loaded into
    """
    try:
        # connect to the PostgreSQL server
        connection = psycopg2.connect(
            user='postgres',
            database="9dt_db",
            password='1234',
            host='localhost',
            port=5432
            )
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    tmp_df = "./tmp_dataframe.csv"
    df.to_csv(tmp_df, index=False, header=False)
    file = open(tmp_df, 'r')
    cursor = connection.cursor()
    try:
        # copy from file to Postgres database
        cursor.copy_from(file, table, sep=",", columns = list(df.columns))
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        file.close()
        os.remove(tmp_df)
        print("Error: %s" % error)
        connection.rollback()
        cursor.close()
        return 1
    print("Data loaded.")
    file.close()
    cursor.close()
    os.remove(tmp_df)

if __name__ == '__main__':
    create_tables()
    games = transform_game_data('https://s3-us-west-2.amazonaws.com/98point6-homework-assets/game_data.csv')
    game_moves = transform_game_moves(games)
    game_results = tranform_game_results(games)
    load_data(game_results, 'game_results')
    load_data(game_moves, 'game_moves')
