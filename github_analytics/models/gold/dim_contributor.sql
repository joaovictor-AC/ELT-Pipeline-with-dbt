-- models/gold/dim_repository.sql

{{ config(materialized='table') }}

with all_activity as (
    select 
        author_login as login, 
        author_date as activity_date, 
        repo_id
    from {{ ref('stg_commits') }}
    
    UNION ALL
    
    select 
        user_login as login, 
        created_at as activity_date, 
        repo_id
    from {{ ref('stg_pull_requests') }}
)
select
    login as contributor_id,
    login,
    min(activity_date) as first_seen_at,
    count(distinct repo_id) as repos_contributed_to,
    count(*) as total_activities
from all_activity
where login != 'unknown' and login is not null
group by 1, 2