-- models/golden/fact_repo_activity.sql

{{ config(
    materialized='table',
) }}

with daily_commits as (
    select
        repo_id,
        cast(author_date as date) as activity_date,
        count(*) as commits_count,
        count(distinct author_login) as unique_committers
    from {{ ref('stg_commits') }}
    group by 1, 2
),
daily_prs as (
    select
        repo_id,
        cast(created_at as date) as activity_date,
        count(*) as prs_opened,
        count(case when is_merged then 1 end) as prs_merged,
        avg(time_to_close_hours) as avg_pr_close_hours
    from {{ ref('stg_pull_requests') }}
    group by 1, 2
),
daily_issues as (
    select
        repo_id,
        cast(created_at as date) as activity_date,
        count(*) as issues_opened,
        count(case when closed_at is not null then 1 end) as issues_closed,
        avg(time_to_close_hours) as avg_issue_close_hours
    from {{ ref('stg_issues') }}
    group by 1, 2
),

repo_date_spine as (
    select repo_id, activity_date from daily_commits
    union distinct
    select repo_id, activity_date from daily_prs
    union distinct
    select repo_id, activity_date from daily_issues
)

select
    s.repo_id,
    cast(strftime(s.activity_date, '%Y%m%d') as integer) as date_id,
    coalesce(c.commits_count, 0) as commits_count,
    coalesce(c.unique_committers, 0) as unique_committers,
    coalesce(p.prs_opened, 0) as prs_opened,
    coalesce(p.prs_merged, 0) as prs_merged,
    p.avg_pr_close_hours,
    coalesce(i.issues_opened, 0) as issues_opened,
    coalesce(i.issues_closed, 0) as issues_closed,
    i.avg_issue_close_hours
from repo_date_spine s
left join daily_commits c on s.repo_id = c.repo_id and s.activity_date = c.activity_date
left join daily_prs p on s.repo_id = p.repo_id and s.activity_date = p.activity_date
left join daily_issues i on s.repo_id = i.repo_id and s.activity_date = i.activity_date