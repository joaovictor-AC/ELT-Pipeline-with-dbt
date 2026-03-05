-- models/gold/dim_date.sql

{{ config(
    materialized='table',
) }}

with date_spine as (
    select
        unnest(generate_series(
            current_date - interval '5 year',
            current_date,
            interval '1 day'
        ))::date as full_date
),

enriched as (
    select
        cast(strftime(full_date, '%Y%m%d') as integer) as date_id,
        full_date,
        extract(year from full_date) as year,
        extract(month from full_date) as month,
        extract(day from full_date) as day,
        dayname(full_date) as day_name,
        monthname(full_date) as month_name,
        extract(dow from full_date) in (0, 6) as is_weekend,
        extract(quarter from full_date) as quarter
    from date_spine
)

select * from enriched