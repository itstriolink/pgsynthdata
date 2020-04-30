/* 
ATP Tennis Rankings, Results, and Stats

Source: https://github.com/JeffSackmann/tennis_atp 
Data License: CC BY-NC-SA 4.0

Tables (see also _README.txt):
* atp_players
* atp_matches_2019 
* atp_matches_2020 
* atp_rankings_10s
* atp_rankings_current
*/


DROP TABLE IF EXISTS atp_players CASCADE;
CREATE TABLE atp_players
(
    player_id    INTEGER NOT NULL PRIMARY KEY,
    first_name   VARCHAR(60),
    last_name    VARCHAR(60),
    hand         VARCHAR(1),
    birth_date   INTEGER,
    country_code VARCHAR(3)
);

DROP TABLE IF EXISTS atp_matches CASCADE;
CREATE TABLE atp_matches
(
    tourney_id         VARCHAR(34) NOT NULL,
    tourney_name       VARCHAR(60) NOT NULL,
    surface            VARCHAR(5)  NOT NULL,
    draw_size          INTEGER     NOT NULL,
    tourney_level      VARCHAR(1)  NOT NULL,
    tourney_date       DATE        NOT NULL,
    match_num          INTEGER     NOT NULL,
    winner_id          INTEGER     NOT NULL REFERENCES atp_players,
    winner_seed        VARCHAR(2),
    winner_entry       VARCHAR(3),
    winner_name        VARCHAR(60) NOT NULL,
    winner_hand        VARCHAR(1)  NULL,
    winner_ht          INTEGER,
    winner_ioc         VARCHAR(3)  NOT NULL,
    winner_age         NUMERIC(13, 10),
    loser_id           INTEGER     NOT NULL REFERENCES atp_players,
    loser_seed         VARCHAR(2),
    loser_entry        VARCHAR(3),
    loser_name         VARCHAR(60) NOT NULL,
    loser_hand         VARCHAR(1),
    loser_ht           INTEGER,
    loser_ioc          VARCHAR(3)  NOT NULL,
    loser_age          NUMERIC(13, 10),
    score              VARCHAR(31) NOT NULL,
    best_of            INTEGER     NOT NULL,
    round              VARCHAR(4)  NOT NULL,
    minutes            INTEGER,
    w_ace              INTEGER,
    w_df               INTEGER,
    w_svpt             INTEGER,
    w_1stIn            INTEGER,
    w_1stWon           INTEGER,
    w_2ndWon           INTEGER,
    w_SvGms            INTEGER,
    w_bpSaved          INTEGER,
    w_bpFaced          INTEGER,
    l_ace              INTEGER,
    l_df               INTEGER,
    l_svpt             INTEGER,
    l_1stIn            INTEGER,
    l_1stWon           INTEGER,
    l_2ndWon           INTEGER,
    l_SvGms            INTEGER,
    l_bpSaved          INTEGER,
    l_bpFaced          INTEGER,
    winner_rank        INTEGER,
    winner_rank_points INTEGER,
    loser_rank         INTEGER,
    loser_rank_points  INTEGER
);


DROP TABLE IF EXISTS atp_matches_2019 CASCADE;
CREATE TABLE atp_matches_2019
(
) INHERITS (atp_matches);


DROP TABLE IF EXISTS atp_matches_2020 CASCADE;
CREATE TABLE atp_matches_2020
(
) INHERITS (atp_matches);


DROP TABLE IF EXISTS atp_rankings CASCADE;
CREATE TABLE atp_rankings
(
    ranking_date   DATE    NOT NULL,
    ranking        INTEGER NOT NULL,
    player_id      INTEGER NOT NULL REFERENCES atp_players,
    ranking_points INTEGER
);


DROP TABLE IF EXISTS atp_rankings_10s CASCADE;
CREATE TABLE atp_rankings_10s
(
) INHERITS (atp_rankings);


DROP TABLE IF EXISTS atp_rankings_current CASCADE;
CREATE TABLE atp_rankings_current
(
) INHERITS (atp_rankings);

-- END --