# 98point6 Data Engineer Homework

## Intro
The task was to design a data warehouse for stakeholders to answer questions
about all the 98point6 Drop Token (9dt) players and games, and generally
explore the data.

I was provided with a CSV file with game data and a player
profile API that returns detailed player information based on the player id.

## To run the code please perform the following:

* Create a fork of this repository before doing a checkout of the code.
* Install Docker and Python 3.6 if needed.
* Open a command line and from the root of the repository and run the following to stand up a Postgres database: `docker run -p 5432:5432 -e POSTGRES_USER="postgres" -e POSTGRES_PASSWORD="1234" -e POSTGRES_DB="9dt_db" -v ./pg-data:/var/lib/postgresql/data --name pg-container postgres`
* Leaving the previous command line open, open a new command line in the root directory and install the packages needed by running `pip install -r requirements.txt`
* Execute the following Python scripts in order:
  - `python Player_Data_ETL.py`
  - `python Game_Data_ETL.py`
  - `python Create_Report_View.py`
* To select from the views, open a command prompt in the root directory and run the following:
  - `docker exec -it pg-container bash`
  - `psql -U postgres -W 9dt_db` and when prompted, enter the password: 1234
* Once in psql, you can run the following:
  - `SELECT * FROM rank_win_first_move;`
  - `SELECT * FROM nationality_games;`
  - `SELECT * FROM marketing_one_game_email;`
