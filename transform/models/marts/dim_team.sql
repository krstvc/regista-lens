with

source_teams as (
    select distinct
        fbref_team_id,
        team_name_raw,
        team_name,
        league
    from {{ ref('stg_fbref__player_season_stats') }}
),

-- Pick the most common raw name per team_id as the display name
ranked as (
    select
        fbref_team_id,
        team_name_raw,
        team_name,
        league,
        row_number() over (
            partition by fbref_team_id
            order by team_name_raw
        ) as rn
    from source_teams
)

select
    {{ dbt_utils.generate_surrogate_key(['fbref_team_id']) }} as team_key,
    fbref_team_id as canonical_team_id,
    team_name_raw as full_name,
    team_name as short_name,
    league,
    -- Source ID columns (NULL placeholders for future sources)
    fbref_team_id,
    cast(null as varchar) as understat_team_id,
    cast(null as varchar) as transfermarkt_team_id
from ranked
where rn = 1
