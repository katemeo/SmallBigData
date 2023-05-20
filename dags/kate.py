from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 20),
    'retries': 1
}


dag = DAG('chess', default_args=default_args, schedule_interval=None)


start_step = BashOperator(
    task_id='start_step',
    bash_command='echo "С БОГОМ"',
    dag=dag,
)


create_schema = PostgresOperator(
    task_id='create_schema',
    sql="CREATE SCHEMA IF NOT EXISTS raw;"
        "CREATE SCHEMA IF NOT EXISTS dds;"
        "CREATE SCHEMA IF NOT EXISTS dm;"
        "CREATE SCHEMA IF NOT EXISTS etl;",
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

chess_games = PostgresOperator(
    task_id='chess_games',
    sql="""
        CREATE TABLE IF NOT EXISTS raw.chess_games (event VARCHAR(255), white VARCHAR(255), black VARCHAR(255), result VARCHAR(10), utc_date DATE, utc_time TIME, white_elo INT, black_elo INT, white_rating_diff FLOAT, black_rating_diff FLOAT, eco VARCHAR(10), opening VARCHAR(255), time_control VARCHAR(50), termination VARCHAR(50), an TEXT);
        CREATE TABLE IF NOT EXISTS dds.chess_games (event VARCHAR(255), white VARCHAR(255), black VARCHAR(255), result VARCHAR(10), utc_date DATE, utc_time TIME, white_elo INT, black_elo INT, white_rating_diff FLOAT, black_rating_diff FLOAT, eco VARCHAR(10), opening VARCHAR(255), time_control VARCHAR(50), termination VARCHAR(50), an TEXT);
        TRUNCATE TABLE raw.chess_games;
        TRUNCATE TABLE dds.chess_games;
        copy raw.chess_games from '/tmp/datasets/chess_games.csv' DELIMITER ',' CSV HEADER;
    """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)


load_data_to_dds = PostgresOperator(
    task_id='load_data_to_dds',
    sql="""CREATE OR REPLACE FUNCTION etl.load_data_to_dds(raw_table_name text, dds_table_name text) RETURNS void 
            LANGUAGE plpgsql AS $function$ BEGIN EXECUTE 'INSERT INTO dds.' || dds_table_name || ' SELECT * FROM raw.' || 
            raw_table_name; END; $function$;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_chess_games_dds = PostgresOperator(
    task_id='load_chess_games_dds',
    sql="SELECT etl.load_data_to_dds('chess_games', 'chess_games');",
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_first_chess = PostgresOperator(
    task_id='load_first_chess',
    sql="""create table if not exists dm.hypothesis_1 as select distinct event, result, white_elo, black_elo, opening from dds.chess_games
            where event like '%Blitz%' and white_elo > 1000 and black_elo > 1000 and result != '1/2-1/2' 
            and (white_elo - black_elo < 50) and (black_elo - white_elo < 50) order by opening;
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_second_chess = PostgresOperator(
    task_id='load_second_chess',
    sql="""create table if not exists dm.hypothesis_2 as select event, result, white_elo, black_elo, opening, an from dds.chess_games
            where event like '%Blitz%' and white_elo > 1000 and black_elo > 1000 and result != '1/2-1/2' 
            and (white_elo - black_elo < 50) and (black_elo - white_elo < 50) and (an like '1. d4%' or an like '1. e4%' or an like '1. d5%' or an like '1. e5%');
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

load_third_chess = PostgresOperator(
    task_id='load_third_chess',
    sql="""create table if not exists dm.hypothesis_3 as select event, result, white_elo, black_elo, opening from dds.chess_games
            where event like '%Blitz%' and white_elo > 1000 and black_elo > 1000 and result != '1/2-1/2';
        """,
    postgres_conn_id='postgres_default',
    database='airflow',
    dag=dag
)

start_step >> create_schema >> chess_games >> load_data_to_dds >> load_chess_games_dds >> [load_first_chess, load_second_chess, load_third_chess]