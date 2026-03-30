# Data Model

## Entity-Relationship Diagram

```mermaid
erDiagram
    dim_season {
        string season_key PK
        string season_label
    }

    dim_competition {
        string competition_key PK
        string competition_name
        string country
    }

    dim_team {
        string team_key PK
        string canonical_team_id
        string full_name
        string short_name
        string country
        string fbref_team_id
        string understat_team_name
        string transfermarkt_team_id
    }

    dim_player {
        string player_key PK
        string canonical_player_id
        string full_name
        string short_name
        string nationality
        string primary_position
        string fbref_player_id
        string understat_player_id
        string transfermarkt_player_id
    }

    fct_player_season_stats {
        string player_season_stats_key PK
        string player_key FK
        string team_key FK
        string competition_key FK
        string season_key FK
        integer minutes
        integer goals
        integer assists
        double fbref_xg
        double understat_xg
        double understat_xg_assist
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
    }

    int_player_xref {
        string fbref_player_id
        string understat_player_id
        string transfermarkt_player_id
        string season
        double match_confidence
    }

    int_team_xref {
        string canonical_team_id
        string fbref_team_id
        string understat_team_name
        string transfermarkt_team_id
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

**`fct_player_season_stats`** — one row per player per team per season. FBref provides core appearance and performance stats (games, minutes, goals, assists, cards). Understat enriches with expected metrics (xG, xA, xG Chain, xG Buildup) via entity resolution. The `_sources` array indicates which sources contributed to each row.

**`fct_player_season_valuations`** — one row per player per team per season. Market values in EUR from Transfermarkt, joined to the star schema via entity resolution.

## Dimension Tables

**`dim_player`** — one row per player. Source IDs from FBref, Understat, and Transfermarkt are populated via `int_player_xref` entity resolution.

**`dim_team`** — one row per team. Source IDs mapped via `int_team_xref` (curated seed file).

**`dim_competition`** — top 5 European leagues (Premier League, La Liga, Bundesliga, Serie A, Ligue 1).

**`dim_season`** — seasons in scope (2023-2024, 2024-2025, 2025-2026).

## Intermediate Tables

**`int_player_xref`** — cross-reference mapping player IDs across sources. Uses deterministic fuzzy matching (Jaro-Winkler on normalized names, date of birth, team/league overlap) with confidence scores.

**`int_team_xref`** — cross-reference mapping team names across sources. Driven by the `team_name_mappings` seed file.

## Data Flow

```
Raw (Bronze)          Staging (Silver)         Intermediate          Marts (Gold)
─────────────         ────────────────         ────────────          ────────────
raw_fbref__*    →  stg_fbref__*          ┐
raw_understat__ →  stg_understat__*      ├→  int_player_xref  →  dim_player
raw_transfermarkt → stg_transfermarkt__* ┘   int_team_xref    →  dim_team
                                                                   dim_competition
                                                                   dim_season
                                                                   fct_player_season_stats
                                                                   fct_player_season_valuations
```

## Phase 2 (planned)

- **`dim_match`** — match dimension with date, home/away teams, score, matchweek
- **`fct_player_match_stats`** — player stats at match-level grain
- **`fct_team_match_stats`** — team stats at match-level grain (requires match-level data from FBref)
