with

competitions as (
    select * from (
        values
            ('Premier League', 'England', 1),
            ('La Liga', 'Spain', 1),
            ('Bundesliga', 'Germany', 1),
            ('Serie A', 'Italy', 1),
            ('Ligue 1', 'France', 1)
    ) as t(competition_name, country, tier)
)

select
    {{ dbt_utils.generate_surrogate_key(['competition_name']) }} as competition_key,
    competition_name,
    country,
    tier
from competitions
