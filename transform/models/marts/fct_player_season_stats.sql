with

staging as (
    select * from {{ ref('stg_fbref__player_season_stats') }}
),

players as (
    select player_key, canonical_player_id from {{ ref('dim_player') }}
),

teams as (
    select team_key, fbref_team_id from {{ ref('dim_team') }}
),

seasons as (
    select season_key, season_label from {{ ref('dim_season') }}
),

competitions as (
    select competition_key, competition_name from {{ ref('dim_competition') }}
),

joined as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'p.player_key', 't.team_key', 's.season_key'
        ]) }} as player_season_stats_key,

        -- Dimension keys
        p.player_key,
        t.team_key,
        c.competition_key,
        s.season_key,

        -- Stats
        stg.games,
        stg.games_starts,
        stg.minutes,
        stg.goals,
        stg.assists,
        stg.goals_non_penalty,
        stg.penalties_scored,
        stg.penalties_attempted,
        stg.yellow_cards,
        stg.red_cards,
        stg.xg,
        stg.npxg,
        stg.xg_assist,
        stg.goals_per90,
        stg.assists_per90,
        stg.goals_assists_per90,
        stg.xg_per90,
        stg.xg_assist_per90,

        -- Source tracking
        ['fbref'] as _sources

    from staging as stg
    inner join players as p on stg.fbref_player_id = p.canonical_player_id
    inner join teams as t on stg.fbref_team_id = t.fbref_team_id
    inner join seasons as s on stg.season = s.season_label
    inner join competitions as c on stg.league = c.competition_name
)

select * from joined
