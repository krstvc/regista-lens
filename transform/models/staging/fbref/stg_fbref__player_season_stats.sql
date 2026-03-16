with

source as (
    select * from {{ source('raw_fbref', 'raw_fbref__player_season_stats') }}
),

cleaned as (
    select
        -- Surrogate key: player + team + season
        {{ dbt_utils.generate_surrogate_key(['player_id', 'team_id', '_season']) }}
            as player_season_team_id,

        -- Identifiers
        player_id as fbref_player_id,
        team_id as fbref_team_id,
        league,
        _season as season,

        -- Names (normalized for downstream matching)
        player_name as player_name_raw,
        {{ normalize_player_name('player_name') }} as player_name,
        team_name as team_name_raw,
        {{ normalize_team_name('team_name') }} as team_name,

        -- Bio
        nationality,
        position,
        -- Parse FBref age format "25-123" → integer years
        case
            when age is not null and age like '%-%'
            then cast(split_part(age, '-', 1) as integer)
            else null
        end as age_years,
        age as age_raw,

        -- Appearances
        cast(games as integer) as games,
        cast(games_starts as integer) as games_starts,
        cast(minutes as integer) as minutes,

        -- Goals & assists
        cast(goals as integer) as goals,
        cast(assists as integer) as assists,
        cast(goals_pens as integer) as goals_non_penalty,
        cast(pens_made as integer) as penalties_scored,
        cast(pens_att as integer) as penalties_attempted,

        -- Discipline
        cast(cards_yellow as integer) as yellow_cards,
        cast(cards_red as integer) as red_cards,

        -- Expected stats
        cast(xg as double) as xg,
        cast(npxg as double) as npxg,
        cast(xg_assist as double) as xg_assist,

        -- Per 90 stats
        cast(goals_per90 as double) as goals_per90,
        cast(assists_per90 as double) as assists_per90,
        cast(goals_assists_per90 as double) as goals_assists_per90,
        cast(xg_per90 as double) as xg_per90,
        cast(xg_assist_per90 as double) as xg_assist_per90,

        -- Multi-team flags
        cast(is_multi_team_total as boolean) as is_multi_team_total,
        cast(is_multi_team_row as boolean) as is_multi_team_row,

        -- Metadata
        _ingested_at,
        _source_url

    from source
),

filtered as (
    -- Remove multi-team total rows (keep per-team breakdowns only)
    -- Remove zero-minute rows (players on roster but never played)
    select *
    from cleaned
    where not is_multi_team_total
      and (minutes is null or minutes > 0)
)

select * from filtered
