with

seasons as (
    select * from (
        values
            ('2023-2024', '2023-08-01'::date, '2024-06-30'::date),
            ('2024-2025', '2024-08-01'::date, '2025-06-30'::date),
            ('2025-2026', '2025-08-01'::date, '2026-06-30'::date)
    ) as t(season_label, start_date, end_date)
)

select
    {{ dbt_utils.generate_surrogate_key(['season_label']) }} as season_key,
    season_label,
    start_date,
    end_date
from seasons
