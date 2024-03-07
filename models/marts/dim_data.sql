with date_array as (
{{
        dbt_utils.date_spine(
            datepart="day"
            , start_date="cast('2011-01-01' as date)"
            , end_date="cast('2016-01-01' as date)"
        )
    }}
)

, casting_fix as (
    select cast(date_day as timestamp) as date_day
    from date_array
)

, dates as (
    select
        {{
            dbt_utils.generate_surrogate_key(['date_day'])
        }} as pk_data
       -- , cast(date(date_day, 'yyyymmdd') as int) as date_dim_id
        , date_day as date_actual
       -- , date_trunc('week', date_day) as reference_week
       -- , to_date(concat(to_char(date_day, 'YYYY-MM'), '-01'), 'YYYY-MM-DD') as reference_month
       -- , extract(epoch from date_day) as epoch
       -- , to_char(date_day, 'fmDDth') as day_suffix
       -- , to_char(date_day, 'Day') as day_name
        -- , extract(isodow from date_day) as day_of_week
        , extract(day from date_day) as day_of_month
         , extract(year from date_day) as year
       ---- , cast(date_day - date_trunc('quarter', date_day + 1) as day_of_quarter
       -- , extract(doy from date_day) as day_of_year
       -- , cast( to_char(date_day, 'W')as int) as week_of_month
       -- , extract(week from date_day) as week_of_year
       -- , to_char(date_day, 'YYYY"-W"IW-') || extract(isodow from date_day) as week_of_year_iso
        , extract(month from date_day) as month_actual
       -- , to_char(date_day, 'Month') as month_name
       -- , to_char(date_day, 'Mon') as month_name_abbreviated
       -- , extract(quarter from date_day) as quarter_actual
       -- , case
       --     when extract(quarter from date_day) = 1 then 'First'
       --     when extract(quarter from date_day) = 2 then 'Second'
       --     when extract(quarter from date_day) = 3 then 'Third'
       --     when extract(quarter from date_day) = 4 then 'Fourth'
       -- end as quarter_name
       -- , extract(isoyear from date_day) as year_actual
       -- , cast( date_day + (1 - extract(isodow from date_day)) as int) as first_day_of_week
       -- , cast( date_day + (7 - extract(isodow from date_day)) as int) as last_day_of_week
       -- , cast( date_day + (1 - extract(day from date_day)) as int) as first_day_of_month
       -- , cast((date_trunc('MONTH', date_day) + interval '1 MONTH - 1 day') as date) as last_day_of_month
       -- , cast (date_trunc('quarter', date_day) as date) as first_day_of_quarter
       -- , cast(
       --     date_trunc(
       --         'quarter', date_day
       --     ) + interval '3 MONTH - 1 day' as date
       -- ) as last_day_of_quarter
       -- , to_date(extract(isoyear from date_day) || '-01-01', 'YYYY-MM-DD') as first_day_of_year
       -- , to_date(extract(isoyear from date_day) || '-12-31', 'YYYY-MM-DD') as last_day_of_year
       -- , to_char(date_day, 'mmyyyy') as mmyyyy
       -- , to_char(date_day, 'mmddyyyy') as mmddyyyy
       --, extract(dow from date_day) in (0, 6) as is_weekend_day
    from casting_fix
    order by date_day desc
)

select *
from dates
