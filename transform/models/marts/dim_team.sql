with

team_xref as (
    select * from {{ ref('int_team_xref') }}
),

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
    {{ dbt_utils.generate_surrogate_key(['coalesce(tx.canonical_team_id, r.fbref_team_id)']) }}
        as team_key,
    coalesce(tx.canonical_team_id, r.fbref_team_id) as canonical_team_id,
    r.team_name_raw as full_name,
    r.team_name as short_name,
    r.league,
    -- Source ID columns
    r.fbref_team_id,
    tx.understat_team_name as understat_team_id,
    tx.transfermarkt_team_id
from ranked r
left join team_xref tx on r.fbref_team_id = tx.fbref_team_id
where r.rn = 1
