-- models/silver/stg_issues.sql

{{ config(
    materialized='view',
) }}

with source as (
    select * from {{ source('bronze', 'raw_issues') }}
),

cleaned as (
    select
        cast(issue_number as INTEGER) as issue_number,
        repo_full_name as repo_id,
        cast(created_at as TIMESTAMP) as created_at,
        cast(closed_at as TIMESTAMP) as closed_at,
        cast(is_pull_request as BOOLEAN) as is_pull_request,
        date_diff('hour',
            cast(created_at as TIMESTAMP),
            cast(closed_at as TIMESTAMP)
        ) as time_to_close_hours
    from source
    where issue_number is not null
        and cast(is_pull_request as BOOLEAN) = false
)

select * from cleaned