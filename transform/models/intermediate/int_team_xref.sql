with

fbref_teams as (
    select distinct
        fbref_team_id,
        team_name,
        league
    from {{ ref('stg_fbref__player_season_stats') }}
),

understat_teams as (
    select distinct
        team_name as understat_team_name,
        league
    from {{ ref('stg_understat__player_season_stats') }}
),

transfermarkt_teams as (
    select distinct
        transfermarkt_team_id,
        team_name as transfermarkt_team_name,
        league
    from {{ ref('stg_transfermarkt__player_valuations') }}
),

seed_mappings as (
    select * from {{ ref('team_name_mappings') }}
),

-- Resolve FBref team names: use seed override if present, else use normalized name directly
fbref_mapped as (
    select
        ft.fbref_team_id,
        ft.team_name as fbref_team_name,
        ft.league,
        coalesce(sm.canonical_team_name, ft.team_name) as canonical_team_name
    from fbref_teams ft
    left join seed_mappings sm
        on sm.source = 'fbref'
        and sm.source_team_name = ft.team_name
        and sm.league = ft.league
),

-- Resolve Understat team names: use seed override if present, else use normalized name directly
understat_mapped as (
    select
        ut.understat_team_name,
        ut.league,
        coalesce(sm.canonical_team_name, ut.understat_team_name) as canonical_team_name
    from understat_teams ut
    left join seed_mappings sm
        on sm.source = 'understat'
        and sm.source_team_name = ut.understat_team_name
        and sm.league = ut.league
),

-- Resolve Transfermarkt team names: use seed override if present, else use normalized name directly
transfermarkt_mapped as (
    select
        tt.transfermarkt_team_id,
        tt.transfermarkt_team_name,
        tt.league,
        coalesce(sm.canonical_team_name, tt.transfermarkt_team_name) as canonical_team_name
    from transfermarkt_teams tt
    left join seed_mappings sm
        on sm.source = 'transfermarkt'
        and sm.source_team_name = tt.transfermarkt_team_name
        and sm.league = tt.league
)

-- Join on canonical name to produce the cross-reference
select
    {{ dbt_utils.generate_surrogate_key(['fm.canonical_team_name', 'fm.league']) }}
        as canonical_team_id,
    fm.fbref_team_id,
    fm.fbref_team_name,
    um.understat_team_name,
    tm.transfermarkt_team_id,
    tm.transfermarkt_team_name,
    fm.canonical_team_name,
    fm.league
from fbref_mapped fm
left join understat_mapped um
    on fm.canonical_team_name = um.canonical_team_name
    and fm.league = um.league
left join transfermarkt_mapped tm
    on fm.canonical_team_name = tm.canonical_team_name
    and fm.league = tm.league
