-- models/golden/dim_repository.sql

{{ config(materialized='table') }}

with repo as (
    select * from {{ ref('stg_repositories') }}
)
select
    repo_id,
    repo_name,
    owner_login,
    description,
    language,
    license_name,
    stars_count,
    forks_count,
    watchers_count,
    created_at,
    repo_age_days,
    default_branch,
    has_wiki,
    has_pages
from repo