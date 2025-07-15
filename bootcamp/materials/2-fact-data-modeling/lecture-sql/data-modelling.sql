# see which kind of details are present there
SELECT 
    game_id, team_id, player_id, COUNT(1) 
    FROM game_details
    GROUP BY 1,2,3
    HAVING  COUNT(1) > 1; --has two of every record

# so to Dedupe it
INSERT INTO fct_game_details
WITH deduped AS(
    SELECT 
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) as row_num
    FROM game_details gd
    JOIN games g on gd.game_id = g.game_id
)
SELECT
    game_date_est as dim_game_date,
    season as dim_season,
    team_id as dim_team_id,
    player_id as dim_player_id,
    player_name as dim_player_name,
    start_position as dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP'in comment),0) > 0 AS dim_did_not_play,
    COALESCE(POSITION('DND'in comment),0) > 0 AS dim_did_not_dress,
    COALESCE(POSITION('NWT'in comment),0) > 0 AS dim_not_with_team,
    CAST(split_part(min, ':', 1) AS REAL )
       + CAST(split_part(min, ':', 2) AS REAL) / 60
    AS m_minutes,
    fgm as m_fgm,
    fga as m_fga,
    fg3a as m_fg3a,
    fg3m as fg3m,
    ftm as m_ftm,
    fta as m_ftm,
    oreb as m_oreb,
    dreb as m_dreb,
    reb as m_reb,
    ast as m_ast,
    stl as m_stl,
    blk as m_blk,
    "TO" AS m_turnovers,
    pf as m_pf,
    pts as m_pts,
    plus_minus as m_plus_minus
FROM deduped
WHERE row_num = 1;

--DDL
CREATE TABLE fct_game_details(
   dim_game_date DATE,
   dim_season_id INTEGER,
   dim_team_id INTEGER,
   dim_player_id INTEGER,
   dim_player_name TEXT,
   dim_start_position TEXT,
   dim_is_playing_at_home BOOLEAN,
   dim_did_not_play BOOLEAN,
   dim_did_not_dress BOOLEAN,
   dim_not_with_team BOOLEAN,
   m_minutes REAL,
   m_fgm INTEGER,
   m_fga INTEGER,
   m_fg3a INTEGER,
   m_fg3m INTEGER,
   m_ftm INTEGER,
   m_fta INTEGER,
   m_oreb INTEGER,
   m_dreb INTEGER,
   m_reb INTEGER,
   m_ast INTEGER,
   m_stl INTEGER,
   m_blk INTEGER,
   m_turnover INTEGER,
   m_pf INTEGER,
   m_pts INTEGER,
   m_plus_minus INTEGER,
   PRIMARY KEY (dim_game_date, dim_player_id, dim_team_id)
)

SELECT * FROM fct_game_details;

SELECT t.*, gdf.* FROM fct_game_details gdf JOIN teams t
ON t.team_id = gdf.dim_team_id;

SELECT 
    dim_player_name, 
    COUNT(1) AS num_games,
    COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS bailed_num,
    CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL) / COUNT(1) AS bailed_pct
FROM fct_game_details
GROUP BY dim_player_name
ORDER BY 4 DESC;
