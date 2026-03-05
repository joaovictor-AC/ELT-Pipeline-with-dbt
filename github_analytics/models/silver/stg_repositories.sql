-- models/silver/stg_repositories.sql

{{ config(
    materialized='view',
) }}

with source as (
    select * from {{ source('bronze', 'raw_repositories') }}
),

cleaned as (
    select
        full_name as repo_id,
        name as repo_name,
        owner_login,
        cast(created_at as TIMESTAMP) as created_at,
        cast(updated_at as TIMESTAMP) as updated_at,
        cast(pushed_at as TIMESTAMP) as pushed_at,
        cast(stargazers_count as INTEGER) as stars_count,
        cast(forks_count as INTEGER) as forks_count,
        cast(watchers_count as INTEGER) as watchers_count,
        coalesce(description, 'No description') as description,
        coalesce(language, 'Unknown') as language,
        license_name,
        default_branch,
        cast(has_wiki as BOOLEAN) as has_wiki,
        cast(has_pages as BOOLEAN) as has_pages,
        date_diff('day', cast(created_at as TIMESTAMP), current_timestamp) as repo_age_days
    from source
    where archived = false 
)
select *
from cleaned
qualify row_number() over (
    partition by repo_id
    order by updated_at desc
) = 1
