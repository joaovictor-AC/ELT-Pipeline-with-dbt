-- models/silver/stg_commits.sql

{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

with source as (
    select * from {{ source('bronze', 'raw_commits') }}
),

cleaned as (
    select
        sha as commit_sha,
        repo_full_name as repo_id,
        coalesce(author_login, 'unknown') as author_login,
        cast(author_date as TIMESTAMP) as author_date,
        cast(committer_date as TIMESTAMP) as committer_date,
        extract(dayofweek from cast(author_date as TIMESTAMP)) as day_of_week,
        extract(hour from cast(author_date as TIMESTAMP)) as hour_of_day,
        left(message, 200) as commit_message
    from source
    where sha is not null
)
select * from cleaned

{% if is_incremental() %}
    where author_date > (select max(author_date) from {{ this }})
{% endif %}