"""Player Data ETL

This script processes 9dt player data from an API into a PostgreSQL database.

This file contains the following functions:

    * create_tables - creates tables in a PostgreSQL database
    * extract_player_data - returns a DataFrame of API data
    * transform_player_data - returns a DataFrame of cleaned player data
    * load_data - loads data to PostgreSQL
    * main - the main function of the script
"""

import psycopg2
import pandas as pd
import datetime as dt
import os
import requests
import json

def create_tables():
    """Creates table(s) in PostgreSQL database"""
    command = """
        CREATE TABLE player_data (
            id SERIAL PRIMARY KEY,
            gender VARCHAR(25) NOT NULL,
            title VARCHAR(25) NOT NULL,
            first VARCHAR(250) NOT NULL,
            last VARCHAR(250) NOT NULL,
            street VARCHAR(250) NOT NULL,
            city VARCHAR(250) NOT NULL,
            state VARCHAR(250) NOT NULL,
            postcode VARCHAR(250) NOT NULL,
            email VARCHAR(250) NOT NULL,
            dob DATE NOT NULL,
            registered DATE NOT NULL,
            phone VARCHAR(250) NOT NULL,
            cell VARCHAR(250) NOT NULL,
            nat VARCHAR(250) NOT NULL,
            insert_dtm timestamp NOT NULL DEFAULT NOW()
        )"""
    connection = None
    try:
        connection = psycopg2.connect(
            user='postgres',
            database="9dt_db",
            password='1234',
            host='localhost',
            port=5432
            )
        cur = connection.cursor()
        cur.execute(command)
        cur.close()
        connection.commit()
        print("Tables created.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

def extract_player_data():
    """Extracts player data from API.

    Returns
    -------
    df1
        a Pandas DataFrame of player data
    """
    JSONContent = requests.get('https://x37sv76kth.execute-api.us-west-1.amazonaws.com/prod/users?page=0').json()
    df1 = pd.json_normalize(JSONContent)
    df1.columns = df1.columns.map(lambda x: x.split(".")[-1])

    page = 1
    while(True):
        print(page)
        try:
            JSONContent = requests.get('https://x37sv76kth.execute-api.us-west-1.amazonaws.com/prod/users?page={}'.format(page)).json()
            df2 = pd.json_normalize(JSONContent)
            df2.columns = df2.columns.map(lambda x: x.split(".")[-1])
            df1 = pd.concat([df1, df2])
            page += 1
            if len(JSONContent) == 0:
                break
        except requests.exceptions.HTTPError as e:
            print("Error: " + str(e))
            break
    return df1

def transform_player_data(df):
    """Cleans player data and returns a DataFrame.

    Parameters
    ----------
    df : DataFrame
        A Pandas DataFrame with player data.

    Returns
    -------
    df
        a Pandas DataFrame of cleaned player data
    """
    df['dob'] = pd.to_datetime(df['dob']).dt.date
    df['registered'] = pd.to_datetime(df['registered'])
    df['city'] = df['city'].replace({',': ''}, regex=True)
    df = df.filter(['id','gender','title','first','last','street','city','state','postcode','email','dob','registered','phone','cell','nat'])
    return df

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
    df.to_csv(tmp_df, index=False, header=False,encoding="utf-8")
    file = open(tmp_df, 'r', encoding="utf-8")
    cursor = connection.cursor()
    try:
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
    cursor.close()
    file.close()
    os.remove(tmp_df)

if __name__ == '__main__':
    create_tables()
    df = extract_player_data()
    player_data = transform_player_data(df)
    load_data(player_data,'player_data')
