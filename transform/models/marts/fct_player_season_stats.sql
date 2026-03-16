with

fbref_staging as (
    select * from {{ ref('stg_fbref__player_season_stats') }}
),

understat_staging as (
    select * from {{ ref('stg_understat__player_season_stats') }}
),

player_xref as (
    select * from {{ ref('int_player_xref') }}
),

team_xref as (
    select * from {{ ref('int_team_xref') }}
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

-- Resolve Understat data to FBref players via xref
understat_resolved as (
    select
        px.fbref_player_id,
        us.season,
        us.xg as understat_xg,
        us.xg_assist as understat_xg_assist,
        us.npxg as understat_npxg,
        us.xg_chain,
        us.xg_buildup,
        us.shots as understat_shots,
        us.key_passes
    from understat_staging us
    inner join player_xref px
        on us.understat_player_id = px.understat_player_id
        and us.season = px.season
    inner join team_xref tx
        on us.team_name = tx.understat_team_name
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

        -- FBref stats
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
        stg.xg as fbref_xg,
        stg.npxg as fbref_npxg,
        stg.xg_assist as fbref_xg_assist,
        stg.goals_per90,
        stg.assists_per90,
        stg.goals_assists_per90,
        stg.xg_per90,
        stg.xg_assist_per90,

        -- Understat stats (NULL if no match)
        ur.understat_xg,
        ur.understat_xg_assist,
        ur.understat_npxg,
        ur.xg_chain,
        ur.xg_buildup,
        ur.understat_shots,
        ur.key_passes,

        -- Source tracking: dynamic based on which joins succeeded
        case
            when ur.fbref_player_id is not null then ['fbref', 'understat']
            else ['fbref']
        end as _sources

    from fbref_staging as stg
    inner join players as p
        on stg.fbref_player_id = p.canonical_player_id
    inner join teams as t on stg.fbref_team_id = t.fbref_team_id
    inner join seasons as s on stg.season = s.season_label
    inner join competitions as c on stg.league = c.competition_name
    left join understat_resolved as ur
        on stg.fbref_player_id = ur.fbref_player_id
        and stg.season = ur.season
)

select * from joined
