REM Create schema in DB tennis_atp_2020
psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -f atp_2020_schema.sql

REM Copy CSV data
psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -c "\copy atp_players FROM 'atp_players.csv' WITH DELIMITER ',' CSV HEADER;"

psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -c "\copy atp_matches_2019 FROM 'atp_matches_2019.csv' WITH DELIMITER ',' CSV HEADER;"

psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -c "\copy atp_matches_2020 FROM 'atp_matches_2020.csv' WITH DELIMITER ',' CSV HEADER;"

psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -c "\copy atp_rankings_10s FROM 'atp_rankings_10s.csv' WITH DELIMITER ',' CSV HEADER;"

psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -c "\copy atp_rankings_current FROM 'atp_rankings_current.csv' WITH DELIMITER ',' CSV HEADER;"

REM Done.