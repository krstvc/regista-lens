with

source as (
    select * from {{ source('raw_transfermarkt', 'raw_transfermarkt__player_valuations') }}
),

cleaned as (
    select
        -- Surrogate key: player + team + season
        {{ dbt_utils.generate_surrogate_key(['player_id', 'team_id', '_season']) }}
            as player_valuation_id,

        -- Identifiers
        player_id as transfermarkt_player_id,
        team_id as transfermarkt_team_id,
        _season as season,

        -- Names (normalized for downstream matching)
        player_name as player_name_raw,
        {{ normalize_player_name('player_name') }} as player_name,
        team_name as team_name_raw,
        {{ normalize_team_name('team_name') }} as team_name,
        league,

        -- Bio
        position,
        nationality,
        case
            when date_of_birth is not null
            then try_cast(date_of_birth as date)
            else null
        end as date_of_birth,
        cast(age as integer) as age_years,

        -- Valuation
        cast(market_value_eur as integer) as market_value_eur,

        -- Metadata
        _ingested_at,
        _source_url

    from source
    qualify row_number() over (
        partition by player_id, team_id, _season
        order by _ingested_at desc
    ) = 1
)

select * from cleaned
