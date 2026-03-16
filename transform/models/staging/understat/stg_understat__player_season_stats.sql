with

source as (
    select * from {{ source('raw_understat', 'raw_understat__player_season_stats') }}
),

cleaned as (
    select
        -- Surrogate key: player + team + season
        {{ dbt_utils.generate_surrogate_key(['player_id', 'team_name', '_season']) }}
            as player_season_team_id,

        -- Identifiers
        player_id as understat_player_id,
        _season as season,

        -- Names (normalized for downstream matching)
        player_name as player_name_raw,
        {{ normalize_player_name('player_name') }} as player_name,
        team_name as team_name_raw,
        {{ normalize_team_name('team_name') }} as team_name,
        league,

        -- Position: map Understat codes to readable groups
        position as position_raw,
        case
            when position is null then null
            when position = 'GK' or position like 'GK %' then 'Goalkeeper'
            when split_part(position, ' ', 1) = 'F' then 'Forward'
            when split_part(position, ' ', 1) = 'M' then 'Midfielder'
            when split_part(position, ' ', 1) = 'D' then 'Defender'
            when position = 'Sub' then 'Substitute'
            else position
        end as position_group,

        -- Appearances
        cast(games as integer) as games,
        cast(minutes as integer) as minutes,

        -- Goals & assists
        cast(goals as integer) as goals,
        cast(assists as integer) as assists,
        cast(npg as integer) as npg,

        -- Expected stats (Understat's core value)
        cast(xg as double) as xg,
        cast(xg_assist as double) as xg_assist,
        cast(npxg as double) as npxg,
        cast(xg_chain as double) as xg_chain,
        cast(xg_buildup as double) as xg_buildup,

        -- Other
        cast(shots as integer) as shots,
        cast(key_passes as integer) as key_passes,
        cast(yellow_cards as integer) as yellow_cards,
        cast(red_cards as integer) as red_cards,

        -- Metadata
        _ingested_at,
        _source_url

    from source
),

filtered as (
    -- Remove zero-minute players (on roster but never played)
    select *
    from cleaned
    where minutes is null or minutes > 0
)

select * from filtered
