-- models/silver/stg_pull_requests.sql

{{ config(
    materialized='incremental',
    unique_key=['repo_id', 'pr_number'],
    incremental_strategy='merge'
) }}

with source as (
    select * from {{ source('bronze', 'raw_pull_requests') }}
),

cleaned as (
    select
        cast(pr_number as INTEGER) as pr_number,
        repo_full_name as repo_id,
        user_login,
        cast(created_at as TIMESTAMP) as created_at,
        cast(merged_at as TIMESTAMP) as merged_at,
        cast(closed_at as TIMESTAMP) as closed_at,
        (merged_at is not null) as is_merged,
        cast(draft as BOOLEAN) as is_draft,
        date_diff('hour',
            cast(created_at as TIMESTAMP),
            coalesce(cast(merged_at as TIMESTAMP), cast(closed_at as TIMESTAMP))
        ) as time_to_close_hours
    from source
    where pr_number is not null
)

select * from cleaned

{% if is_incremental() %}
    where coalesce(closed_at, merged_at, created_at) > (
        select max(coalesce(closed_at, merged_at, created_at)) from {{ this }}
    )
{% endif %}