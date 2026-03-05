-- models/silver/stg_pull_requests.sql

{{ config(
    materialized='incremental',
    unique_key='pr_number',
    incremental_strategy='merge'
) }}

with source as (

    select *
    from {{ source('bronze', 'raw_pull_requests') }}

),

cleaned as (

    select
        cast(pr_number as integer) as pr_number,
        repo_full_name as repo_id,
        user_login,
        cast(created_at as timestamp) as created_at,
        cast(merged_at as timestamp) as merged_at,
        cast(closed_at as timestamp) as closed_at,

        (merged_at is not null) as is_merged,
        cast(draft as boolean) as is_draft,

        date_diff(
            'hour',
            cast(created_at as timestamp),
            coalesce(
                cast(merged_at as timestamp),
                cast(closed_at as timestamp)
            )
        ) as time_to_close_hours

    from source
    where pr_number is not null

),

deduplicated as (

    select *
    from cleaned

    qualify row_number() over (
        partition by pr_number
        order by created_at desc
    ) = 1

)

select *
from deduplicated

{% if is_incremental() %}
where coalesce(closed_at, merged_at, created_at) >
      (
        select max(coalesce(closed_at, merged_at, created_at))
        from {{ this }}
      )
{% endif %}
