with

player_xref as (
    select * from {{ ref('int_player_xref') }}
),

source_players as (
    select
        fbref_player_id,
        player_name_raw,
        player_name,
        nationality,
        position,
        age_years,
        season,
        row_number() over (
            partition by fbref_player_id
            order by season desc
        ) as rn
    from {{ ref('stg_fbref__player_season_stats') }}
),

-- Use the most recent season's attributes for each player
latest as (
    select * from source_players where rn = 1
),

-- Get the most recent xref entry per player (for understat_id + transfermarkt_id)
xref_latest as (
    select
        canonical_player_id,
        fbref_player_id,
        understat_player_id,
        transfermarkt_player_id,
        row_number() over (
            partition by fbref_player_id
            order by season desc
        ) as rn
    from player_xref
),

xref_deduped as (
    select * from xref_latest where rn = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['x.canonical_player_id']) }} as player_key,
    x.canonical_player_id,
    l.player_name_raw as full_name,
    l.player_name as short_name,
    l.nationality,
    l.position as primary_position,
    l.age_years,
    -- Source ID columns
    l.fbref_player_id,
    x.understat_player_id as understat_id,
    x.transfermarkt_player_id as transfermarkt_id
from latest l
inner join xref_deduped x on l.fbref_player_id = x.fbref_player_id
