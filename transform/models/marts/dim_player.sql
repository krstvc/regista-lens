with

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
)

select
    {{ dbt_utils.generate_surrogate_key(['fbref_player_id']) }} as player_key,
    fbref_player_id as canonical_player_id,
    player_name_raw as full_name,
    player_name as short_name,
    nationality,
    position as primary_position,
    age_years,
    -- Source ID columns (NULL placeholders for future sources)
    fbref_player_id,
    cast(null as varchar) as understat_id,
    cast(null as varchar) as transfermarkt_id
from latest
