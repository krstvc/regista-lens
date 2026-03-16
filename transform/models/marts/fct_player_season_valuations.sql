with

transfermarkt_staging as (
    select * from {{ ref('stg_transfermarkt__player_valuations') }}
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
    select team_key, canonical_team_id from {{ ref('dim_team') }}
),

seasons as (
    select season_key, season_label from {{ ref('dim_season') }}
),

competitions as (
    select competition_key, competition_name from {{ ref('dim_competition') }}
),

-- Resolve Transfermarkt players to canonical IDs via xref
resolved as (
    select
        ts.transfermarkt_player_id,
        ts.transfermarkt_team_id,
        ts.season,
        ts.league,
        ts.market_value_eur,
        ts.date_of_birth,
        ts.nationality,
        px.canonical_player_id,
        tx.canonical_team_id
    from transfermarkt_staging ts
    inner join player_xref px
        on ts.transfermarkt_player_id = px.transfermarkt_player_id
        and ts.season = px.season
    left join team_xref tx
        on ts.transfermarkt_team_name = tx.transfermarkt_team_name
        and ts.league = tx.league
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'p.player_key', 't.team_key', 's.season_key'
        ]) }} as valuation_key,

        -- Dimension keys
        p.player_key,
        t.team_key,
        s.season_key,
        c.competition_key,

        -- Valuation data
        r.market_value_eur,
        r.date_of_birth,
        r.nationality

    from resolved r
    inner join players p on r.canonical_player_id = p.canonical_player_id
    inner join teams t on r.canonical_team_id = t.canonical_team_id
    inner join seasons s on r.season = s.season_label
    inner join competitions c on r.league = c.competition_name
)

select * from joined
