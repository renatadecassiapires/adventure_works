with date_array as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2011-01-01' as date)",
            end_date="cast('2016-01-01' as date)"
        )
    }}
),

casting_fix as (
    select cast(date_day as timestamp) as date_day
    from date_array
),

dates as (
    select
        {{
            dbt_utils.generate_surrogate_key(['date_day'])
        }} as pk_data,
        cast(date_day as date) as date_actual,
        extract(day from date_day) as day_of_month,
        extract(year from date_day) as year,
        extract(month from date_day) as month_actual
    from casting_fix
    order by date_day desc
)

select *
from dates
