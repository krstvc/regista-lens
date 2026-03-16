with

-- Deduplicate FBref to one row per player per season (use primary team = most minutes)
fbref_players as (
    select
        fbref_player_id,
        player_name,
        team_name as fbref_team_name,
        fbref_team_id,
        league,
        season,
        position,
        row_number() over (
            partition by fbref_player_id, season
            order by minutes desc nulls last
        ) as rn
    from {{ ref('stg_fbref__player_season_stats') }}
),

fbref_deduped as (
    select
        fbref_player_id,
        player_name as fbref_name,
        fbref_team_name,
        fbref_team_id,
        league,
        season,
        position as fbref_position,
        string_split(player_name, ' ')[-1] as last_name
    from fbref_players
    where rn = 1
),

-- Deduplicate Understat to one row per player per season
understat_players as (
    select
        understat_player_id,
        player_name,
        team_name as understat_team_name,
        league,
        season,
        position_group,
        row_number() over (
            partition by understat_player_id, season
            order by minutes desc nulls last
        ) as rn
    from {{ ref('stg_understat__player_season_stats') }}
),

understat_deduped as (
    select
        understat_player_id,
        player_name as understat_name,
        understat_team_name,
        league,
        season,
        position_group as understat_position,
        string_split(player_name, ' ')[-1] as last_name
    from understat_players
    where rn = 1
),

-- Deduplicate Transfermarkt to one row per player per season
transfermarkt_players as (
    select
        transfermarkt_player_id,
        player_name,
        team_name as transfermarkt_team_name,
        league,
        season,
        position,
        row_number() over (
            partition by transfermarkt_player_id, season
            order by market_value_eur desc nulls last
        ) as rn
    from {{ ref('stg_transfermarkt__player_valuations') }}
),

transfermarkt_deduped as (
    select
        transfermarkt_player_id,
        player_name as transfermarkt_name,
        transfermarkt_team_name,
        league,
        season,
        position as transfermarkt_position,
        string_split(player_name, ' ')[-1] as last_name
    from transfermarkt_players
    where rn = 1
),

team_xref as (
    select * from {{ ref('int_team_xref') }}
),

-- === Understat matching (unchanged) ===

-- Block on (normalized last name, league, season) to reduce comparison space
understat_candidates as (
    select
        fp.fbref_player_id,
        fp.fbref_name,
        fp.fbref_team_name,
        fp.fbref_team_id,
        fp.fbref_position,
        fp.league,
        fp.season,
        up.understat_player_id,
        up.understat_name,
        up.understat_team_name,
        up.understat_position,

        -- Name similarity (weight: 0.6)
        jaro_winkler_similarity(fp.fbref_name, up.understat_name) as name_score,

        -- Team overlap via xref (weight: 0.3)
        case
            when tx.canonical_team_id is not null then 1.0
            else 0.0
        end as team_score,

        -- Position compatibility (weight: 0.1)
        case
            when fp.fbref_position is null or up.understat_position is null then 0.5
            when fp.fbref_position like '%FW%' and up.understat_position = 'Forward' then 1.0
            when fp.fbref_position like '%MF%' and up.understat_position = 'Midfielder' then 1.0
            when fp.fbref_position like '%DF%' and up.understat_position = 'Defender' then 1.0
            when fp.fbref_position like '%GK%' and up.understat_position = 'Goalkeeper' then 1.0
            else 0.0
        end as position_score

    from fbref_deduped fp
    inner join understat_deduped up
        on fp.last_name = up.last_name
        and fp.league = up.league
        and fp.season = up.season
    left join team_xref tx
        on fp.fbref_team_id = tx.fbref_team_id
        and up.understat_team_name = tx.understat_team_name
),

understat_scored as (
    select
        *,
        (0.6 * name_score + 0.3 * team_score + 0.1 * position_score) as composite_score
    from understat_candidates
),

understat_best as (
    select
        *,
        row_number() over (
            partition by fbref_player_id, season
            order by composite_score desc
        ) as match_rn
    from understat_scored
    where composite_score >= 0.75
),

understat_matched as (
    select
        fbref_player_id,
        understat_player_id,
        fbref_name,
        understat_name,
        league,
        season,
        composite_score as understat_confidence,
        name_score as understat_name_score,
        team_score as understat_team_score,
        position_score as understat_position_score,
        case
            when composite_score >= 0.90 then 'auto_matched'
            else 'review_needed'
        end as understat_match_status
    from understat_best
    where match_rn = 1
),

-- === Transfermarkt matching ===

transfermarkt_candidates as (
    select
        fp.fbref_player_id,
        fp.fbref_name,
        fp.fbref_team_name,
        fp.fbref_team_id,
        fp.fbref_position,
        fp.league,
        fp.season,
        tp.transfermarkt_player_id,
        tp.transfermarkt_name,
        tp.transfermarkt_team_name,
        tp.transfermarkt_position,

        -- Name similarity (weight: 0.6)
        jaro_winkler_similarity(fp.fbref_name, tp.transfermarkt_name) as name_score,

        -- Team overlap via xref (weight: 0.3)
        case
            when tx.canonical_team_id is not null then 1.0
            else 0.0
        end as team_score,

        -- Position compatibility (weight: 0.1)
        case
            when fp.fbref_position is null or tp.transfermarkt_position is null then 0.5
            when fp.fbref_position like '%FW%' and tp.transfermarkt_position in ('Centre-Forward', 'Left Winger', 'Right Winger', 'Second Striker') then 1.0
            when fp.fbref_position like '%MF%' and tp.transfermarkt_position in ('Central Midfield', 'Attacking Midfield', 'Defensive Midfield', 'Left Midfield', 'Right Midfield') then 1.0
            when fp.fbref_position like '%DF%' and tp.transfermarkt_position in ('Centre-Back', 'Left-Back', 'Right-Back') then 1.0
            when fp.fbref_position like '%GK%' and tp.transfermarkt_position = 'Goalkeeper' then 1.0
            else 0.0
        end as position_score

    from fbref_deduped fp
    inner join transfermarkt_deduped tp
        on fp.last_name = tp.last_name
        and fp.league = tp.league
        and fp.season = tp.season
    left join team_xref tx
        on fp.fbref_team_id = tx.fbref_team_id
        and tp.transfermarkt_team_name = tx.transfermarkt_team_name
),

transfermarkt_scored as (
    select
        *,
        (0.6 * name_score + 0.3 * team_score + 0.1 * position_score) as composite_score
    from transfermarkt_candidates
),

transfermarkt_best as (
    select
        *,
        row_number() over (
            partition by fbref_player_id, season
            order by composite_score desc
        ) as match_rn
    from transfermarkt_scored
    where composite_score >= 0.75
),

transfermarkt_matched as (
    select
        fbref_player_id,
        transfermarkt_player_id,
        league,
        season,
        composite_score as transfermarkt_confidence
    from transfermarkt_best
    where match_rn = 1
),

-- Manual overrides from seed
overrides as (
    select * from {{ ref('player_match_overrides') }}
),

-- All FBref players (matched or not) with override application
all_fbref_players as (
    select distinct
        fbref_player_id,
        league,
        season
    from fbref_deduped
),

final as (
    select
        coalesce(o.canonical_player_id, afp.fbref_player_id) as canonical_player_id,
        afp.fbref_player_id,
        coalesce(o.understat_player_id, um.understat_player_id) as understat_player_id,
        coalesce(o.transfermarkt_player_id, tm.transfermarkt_player_id) as transfermarkt_player_id,
        afp.league,
        afp.season,
        um.understat_confidence as confidence,
        um.understat_name_score as name_score,
        um.understat_team_score as team_score,
        um.understat_position_score as position_score,
        tm.transfermarkt_confidence,
        case
            when o.fbref_player_id is not null then 'manual_override'
            when um.fbref_player_id is not null then um.understat_match_status
            else 'unmatched'
        end as match_status
    from all_fbref_players afp
    left join understat_matched um
        on afp.fbref_player_id = um.fbref_player_id
        and afp.season = um.season
    left join transfermarkt_matched tm
        on afp.fbref_player_id = tm.fbref_player_id
        and afp.season = tm.season
    left join overrides o
        on afp.fbref_player_id = o.fbref_player_id
)

select * from final
