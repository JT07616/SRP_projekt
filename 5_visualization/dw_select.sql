USE tenis;

SELECT
    fp.fact_player_match_tk AS record_id, -- tehnicki ID ostaje samo za COUNT zapisa u Tableau
    dp.name AS player_name,
    dp.hand AS player_hand,
    dp.height AS player_height,
    dp.country_name AS player_country_name,
    dp.region AS player_region,
    dop.name AS opponent_name,
    dop.hand AS opponent_hand,
    dop.height AS opponent_height,
    dop.country_name AS opponent_country_name,
    dop.region AS opponent_region,
    dc.name AS country_name,
    dc.population AS country_population,
    dc.region AS country_region,
    dt.name AS tournament_name,
    dt.surface,
    dt.draw_size,
    dt.level AS tournament_level,
    dt.city AS tournament_city,
    dt.country_name AS tournament_country_name,
    dt.region AS tournament_region,
    dd.date AS full_date,
    dd.day,
    dd.year,
    dd.quarter,
    dd.month,
    dmi.score,
    dmi.round,
    dmi.best_of,
    dmi.seed,
    dmi.entry,
    dmi.rank,
    fp.is_winner,
    fp.ace,
    fp.double_fault,
    fp.service_points,
    fp.first_in,
    fp.first_won,
    fp.second_won,
    fp.service_games,
    fp.break_points_saved,
    fp.break_points_faced,
    fp.rank_points,
    fp.minutes
FROM fact_player_match fp
LEFT JOIN dim_player dp
    ON fp.player_tk = dp.player_tk
LEFT JOIN dim_player dop
    ON fp.opponent_player_tk = dop.player_tk
LEFT JOIN dim_country dc
    ON fp.country_tk = dc.country_tk
LEFT JOIN dim_tournament dt
    ON fp.tournament_tk = dt.tournament_tk
LEFT JOIN dim_date dd
    ON fp.date_tk = dd.date_tk
LEFT JOIN dim_match_info dmi
    ON fp.match_info_tk = dmi.match_info_tk
ORDER BY fp.fact_player_match_tk ASC;
