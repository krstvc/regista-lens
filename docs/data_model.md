# Data Model

## Entity-Relationship Diagram

```mermaid
erDiagram
    dim_season {
        string season_key PK
        string season_label
        date start_date
        date end_date
    }

    dim_competition {
        string competition_key PK
        string competition_name
        string country
        integer tier
    }

    dim_team {
        string team_key PK
        string canonical_team_id
        string full_name
        string short_name
        string league
        string fbref_team_id
        string understat_team_id
        string transfermarkt_team_id
    }

    dim_player {
        string player_key PK
        string canonical_player_id
        string full_name
        string short_name
        string nationality
        string primary_position
        integer age_years
        string fbref_player_id
        string understat_id
        string transfermarkt_id
    }

    fct_player_season_stats {
        string player_season_stats_key PK
        string player_key FK
        string team_key FK
        string competition_key FK
        string season_key FK
        integer games
        integer games_starts
        integer minutes
        integer goals
        integer assists
        integer goals_non_penalty
        integer penalties_scored
        integer penalties_attempted
        integer yellow_cards
        integer red_cards
        double fbref_xg
        double fbref_npxg
        double fbref_xg_assist
        double goals_per90
        double assists_per90
        double goals_assists_per90
        double xg_per90
        double xg_assist_per90
        double understat_xg
        double understat_xg_assist
        double understat_npxg
        integer understat_shots
        integer key_passes
        double xg_chain
        double xg_buildup
        array _sources
    }

    fct_player_season_valuations {
        string valuation_key PK
        string player_key FK
        string team_key FK
        string season_key FK
        string competition_key FK
        bigint market_value_eur
        date date_of_birth
        string nationality
    }

    int_player_xref {
        string canonical_player_id
        string fbref_player_id
        string understat_player_id
        string transfermarkt_player_id
        string league
        string season
        double confidence
        double name_score
        double team_score
        double position_score
        double transfermarkt_confidence
        string match_status
    }

    int_team_xref {
        string canonical_team_id
        string fbref_team_id
        string fbref_team_name
        string understat_team_name
        string transfermarkt_team_id
        string transfermarkt_team_name
        string canonical_team_name
        string league
    }

    dim_player ||--o{ fct_player_season_stats : "player_key"
    dim_team ||--o{ fct_player_season_stats : "team_key"
    dim_competition ||--o{ fct_player_season_stats : "competition_key"
    dim_season ||--o{ fct_player_season_stats : "season_key"

    dim_player ||--o{ fct_player_season_valuations : "player_key"
    dim_team ||--o{ fct_player_season_valuations : "team_key"
    dim_season ||--o{ fct_player_season_valuations : "season_key"
    dim_competition ||--o{ fct_player_season_valuations : "competition_key"
```

## Fact Tables

**`fct_player_season_stats`** — one row per player per team per season. FBref provides core appearance and performance stats (games, minutes, goals, assists, cards, per-90 metrics, xG). Understat enriches with expected metrics (xG, xA, npxG, xG Chain, xG Buildup) via entity resolution. The `_sources` array indicates which sources contributed to each row.

**`fct_player_season_valuations`** — one row per player per team per season. Market values in EUR from Transfermarkt, joined to the star schema via entity resolution. Includes player date of birth and nationality from the Transfermarkt source.

## Dimension Tables

**`dim_player`** — one row per player. Source IDs from FBref, Understat, and Transfermarkt are populated via `int_player_xref` entity resolution. Uses the latest season's attributes for each player.

**`dim_team`** — one row per team. Source IDs mapped via `int_team_xref` (curated seed file).

**`dim_competition`** — top 5 European leagues (Premier League, La Liga, Bundesliga, Serie A, Ligue 1). Hardcoded dimension.

**`dim_season`** — seasons in scope (2023-2024, 2024-2025, 2025-2026). Hardcoded dimension with start and end dates.

## Intermediate Tables

**`int_player_xref`** — cross-reference mapping player IDs across sources. Uses deterministic fuzzy matching with Jaro-Winkler name similarity, team overlap via `int_team_xref`, and position compatibility. Outputs confidence scores and match status (`auto_matched` >= 0.90, `review_needed` >= 0.75). Manual overrides applied from the `player_match_overrides` seed.

**`int_team_xref`** — cross-reference mapping team names and IDs across sources. Driven by the `team_name_mappings` seed file with fallback to normalized source names.

## Data Flow

```
Raw (Bronze)              Staging (Silver)              Intermediate              Marts (Gold)
──────────────            ──────────────────            ────────────              ────────────

raw_fbref__*         →  stg_fbref__*             ┐
                                                  ├→  int_player_xref  ──┐
raw_understat__*     →  stg_understat__*         ┘                      ├→  dim_player
                                                                        │
raw_transfermarkt__* →  stg_transfermarkt__*  ───────────────────────────┤
                                                                        │
                        team_name_mappings (seed) →  int_team_xref  ────┼→  dim_team
                                                                        │
                                              (hardcoded)  ─────────────┼→  dim_competition
                                              (hardcoded)  ─────────────┼→  dim_season
                                                                        │
                        stg_fbref__*  ──────────────────────────────────┼→  fct_player_season_stats
                        stg_understat__*  + int_player_xref  ───────────┤
                                                                        │
                        stg_transfermarkt__* + int_player_xref  ────────┴→  fct_player_season_valuations
```

## Phase 2 (planned)

- **`dim_match`** — match dimension with date, home/away teams, score, matchweek
- **`fct_player_match_stats`** — player stats at match-level grain
- **`fct_team_match_stats`** — team stats at match-level grain (requires match-level data from FBref)
