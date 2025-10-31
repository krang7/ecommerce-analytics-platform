{{
    config(
        materialized='table',
        description= 'Date dimension for time-based analysis'
    )
}}

with date_spine as (
    -- Generate all dates from 2016 to 2020 (covers your data range)
    select
        date_day::date as date_day
    from 
        generate_series(
            '2016-01-01'::date,
            '2020-12-31'::date,
            '1 day'::interval
        ) as date_day
),

final as (
    select
        -- Date
        date_day,
        
        -- Year
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        extract(week from date_day) as week_of_year,
        extract(day from date_day) as day_of_month,
        extract(dow from date_day) as day_of_week, -- 0=Sunday, 6=Saturday
        extract(doy from date_day) as day_of_year,
        
        -- Formatted dates
        to_char(date_day, 'YYYY-MM-DD') as date_formatted,
        to_char(date_day, 'YYYY-MM') as year_month,
        to_char(date_day, 'YYYY-Q') as year_quarter,
        
        -- Names
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Mon') as month_name_short,
        
        -- Flags
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend,
        case when extract(dow from date_day) between 1 and 5 then true else false end as is_weekday,
        
        -- Relative dates
        case when date_day = current_date then true else false end as is_today,
        case when date_day = current_date - interval '1 day' then true else false end as is_yesterday,
        case when date_day >= date_trunc('month', current_date) then true else false end as is_current_month,
        case when date_day >= date_trunc('year', current_date) then true else false end as is_current_year,
        
        -- Holiday markers (add as needed for your region)
        case 
            when extract(month from date_day) = 12 and extract(day from date_day) = 25 then 'Christmas'
            when extract(month from date_day) = 1 and extract(day from date_day) = 1 then 'New Year'
            when extract(month from date_day) = 11 and extract(day from date_day) = 15 then 'Black Friday (approx)'
            else null
        end as holiday_name

    from date_spine
)

select * from final