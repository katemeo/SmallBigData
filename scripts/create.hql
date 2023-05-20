CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS archive;

CREATE TABLE IF NOT EXISTS ods.chess_games ( event STRING, white STRING, black STRING, result STRING, utc_date DATE, utc_time STRING, white_elo INT, black_elo INT, white_rating_diff FLOAT, black_rating_diff FLOAT, eco STRING, opening STRING, time_control STRING, termination STRING, an STRING)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/chess_games';

CREATE TABLE IF NOT EXISTS archive.chess_games ( event STRING, white STRING, black STRING, result STRING, utc_date DATE, utc_time STRING, white_elo INT, black_elo INT, white_rating_diff FLOAT, black_rating_diff FLOAT, eco STRING, opening STRING, time_control STRING, termination STRING, an STRING)
row format delimited
fields terminated by ','
lines terminated by '\n'
stored as textfile location 'hdfs://namenode:8020/user/hive/warehouse/ods.db/chess_games';