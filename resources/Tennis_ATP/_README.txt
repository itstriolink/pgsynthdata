============================================
Tennis ATP Rankings, Results, and Statistics
============================================

Updated: 2020-04-14 sfkeller

This is a dataset containing master ATP player file, historical rankings, results, and match stats.
This data mainly covers 2019 and 2020 (see atp_matches_* and atp_rankings_*).

Credits: Jeff Sackmann

Sources: 
* Data: https://github.com/JeffSackmann/tennis_atp 
* Data License: CC BY-NC-SA 4.0
* ATP Tour: https://de.wikipedia.org/wiki/ATP_Tour_2019
* See also https://github.com/awesomedata/awesome-public-datasets#sports 


DESCRIPTION 
-----------

Files/tables:
* Players:  atp_players.csv
* Matches:  atp_matches_2019, atp_matches_2020
* Rankings: atp_rankings_10s, atp_rankings_current

These are the main entities:
* player 
  * with columns player_id, first_name, last_name, hand, birth_date, country_code. 
  * where hand is in {'A','R','L','U',NULL}.
* matches: see file (_README_)'matches_data_dictionary.txt', where 
  * surface is in {'Hard','Clay','Grass'}.
  * turney_level is in {A,D,F,G,M}
  * winner_hand, looser_hand - see player.hand
  * winner_entry, looser_entry is in {NULL,Q,WC,PR,LL,SE}
* ranking columns are 
  * ranking_date, ranking, player_id, ranking_points (where available); where 
  * ranking_date in 2020-01-06,2020-01-13,2020-01-20,2020-02-03,2020-02-10,2020-02-17,2020-02-24,2020-03-02,2020-03-09

Lineage: 

Following steps have been made by sfkeller to prepare this dataset for PostgreSQL:
* Downloaded from https://github.com/JeffSackmann/tennis_atp
* Extracted the 4 files mentioned above.
* Added headers to player and matches CSV file. 
* Instead of importing the CSVs directly - as shown below (which involves creating a schema by hand) a CSV file of each entity has been concerted to SQL using this online tool: https://www.convertcsv.com/csv-to-sql.htm . This doesn't work if CSV file sizes are larger than some megabytes. 
* Then imported these SQL INSERTs like this, e.g.:
% psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -f atp_players.sql


DB Schema:
                            List of relations
 Schema |         Name         | Type  |  Owner   |  Size   | ROWS BY KES
--------+----------------------+-------+----------+---------+-------------
 public | atp_matches_2019     | table | postgres | 792 kB  |   2781, no PK 
 public | atp_matches_2020     | table | postgres | 232 kB  |    747, no PK
 public | atp_players          | table | postgres | 3264 kB |  54938
 public | atp_rankings_10s     | table | postgres | 39 MB   | 916296
 public | atp_rankings_current | table | postgres | 784 kB  |  17401



INSTALLING / IMPORTING THE DATABASE
-----------------------------------

To import the dataset/database:
* Download zip file ("Tennis_ATP.zip)".
* Create database 'tennis_atp_2020' in PostgreSQL: 
  % psql -U postgres -d tennis_atp_2020
  tennis_atp_2020=# CREATE DATABASE tennis_atp_2020;
* Create schema "atp_2020_schema.sql"
  % psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -f atp_2020_schema.sql
* Do CSV imports!

For CSV imports:
* Change to directory of dataset.

* A. On Windows CMD shell, run > import.bat < which calls psql as described below!

* B. Do this CMD shell for each CSV (which is the content of import.bat):
psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -c "\copy atp_players FROM 'atp_players.csv' WITH DELIMITER ',' CSV HEADER;"
psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -c "\copy atp_matches_2019 FROM 'atp_matches_2019.csv' WITH DELIMITER ',' CSV HEADER;"
psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -c "\copy atp_matches_2020 FROM 'atp_matches_2020.csv' WITH DELIMITER ',' CSV HEADER;"
psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -c "\copy atp_rankings_10s FROM 'atp_rankings_10s.csv' WITH DELIMITER ',' CSV HEADER;"
psql -U postgres -d tennis_atp_2020 -v ON_ERROR_STOP=on -c "\copy atp_rankings_current FROM 'atp_rankings_current.csv' WITH DELIMITER ',' CSV HEADER;"

* C. or connect to tennis_atp_2020:
  % psql -U postgres -d tennis_atp_2020
  Then do on tennis_atp_2020=# 
\copy atp_players          FROM 'atp_players.csv'          WITH DELIMITER ',' CSV HEADER;
\copy atp_matches_2019     FROM 'atp_matches_2019.csv'     WITH DELIMITER ',' CSV HEADER;
\copy atp_matches_2020     FROM 'atp_matches_2020.csv'     WITH DELIMITER ',' CSV HEADER;
\copy atp_rankings_10s     FROM 'atp_rankings_10s.csv'     WITH DELIMITER ',' CSV HEADER;
\copy atp_rankings_current FROM 'atp_rankings_current.csv' WITH DELIMITER ',' CSV HEADER;




QUERY EXAMPLES
--------------

-- Swiss Tennis players who participated on a tourney 2019
select distinct tourney_date, tourney_name, last_name, hand, date(birth_date::text) as birth_date 
from atp_matches_2019 as "match" 
join atp_players as player on player.player_id in ("match".winner_id,"match".loser_id) 
where "match".tourney_name not like '%Cup%'
and country_code = 'SUI'
order by tourney_date desc, tourney_name, last_name;


-- All Tennis players who won an ATP tourney except country cups
select distinct tourney_date, tourney_name, winner_name, winner_ioc, winner_hand, floor(winner_age) as "winner_ag"
from atp_matches_2019 as "match" 
join atp_players as player on player.player_id = "match".winner_id
where "match".winner_id = (
	select match2.winner_id
	from atp_matches_2019 as match2
	where match2.tourney_id = "match".tourney_id
	group by match2.winner_id 
	order by count(*) desc limit 1
)
and "match".tourney_name not like '%Cup%'
order by tourney_date desc

-- Tennis players ranked no.1 as count of weekly ranking updates 2019 
select last_name, count(*) as count_of_no1_rankings 
from atp_players as player
join atp_rankings_10s as ranking on player.player_id = ranking.player_id 
where ranking.ranking=1 
group by player.last_name
order by count(*) desc;



Accessing statistics for the planner
------------------------------------

Requirements: Connect to database tennis_atp_2020.

-- Prepare
vacuum (analyze);  -- requires admin rights (user postgres)
analyze;

-- Show infos about a table/relation, e.g. atp_matches_2019:
select nspname, relname, reltuples, relhasindex, relispartition
from pg_class
join pg_namespace nsp on nsp.oid = relnamespace
where relkind='r' and relname = 'atp_matches_2019'

-- Show stats about a table/relation, e.g. atp_matches_2019:
select attname, null_frac, avg_width, n_distinct, most_common_vals, most_common_freqs, histogram_bounds, correlation 
from pg_stats 
where schemaname not in ('pg_catalog') and tablename = 'atp_matches_2019'


Extended statistics for the planner
-----------------------------------

Creating extended statistics about dependencies (and ndistinct and MCV):
Docs: https://www.postgresql.org/docs/current/sql-createstatistics.html

Requirements: Connect to database tennis_atp_2020.


-- drop statistics if exists atp_matches_2019_tourney_id_tourney_name;
create statistics atp_matches_2019_tourney_id_tourney_name (dependencies) 
	on tourney_id, tourney_name 
	from atp_matches_2019;

analyze atp_matches_2019; -- update extended statistics

select * from pg_statistic_ext; -- now shows the entry of "atp_matches_2019_tourney_id_tourney_name"

